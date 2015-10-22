/*
 * #%L
 * Wisdom-Framework
 * %%
 * Copyright (C) 2013 - 2014 Wisdom Framework
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package org.wisdom.framework.jpa;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.UnmarshallerHandler;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.felix.ipojo.Factory;
import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Context;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.util.tracker.BundleTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wisdom.framework.jpa.model.Persistence;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;

import com.google.common.base.Splitter;

/**
 * The entry point of the JPA bridge.
 * This component tracks bundles and check if they contain a {@code Meta-Persistence} header. If so, is creates a
 * necessary persistence unit. By default, the tracker check for {@code META-INF/persistence.xml}
 */
@Component(immediate = true)
@Instantiate
public class JPAManager {

    /**
     * This XML Filter changes the namespace URI of the persistence.xml to the new version 2.1, regardless of the URI
     * used. This is required because the wisdom-jps {@link Persistence} model is generated from the
     * {@code persistence_2_1.xsd} definition, which will not unmarshal pre-2.1 persistence definitions correctly when
     * the old namespace is used.
     * 
     * @author Thomas Wunschel <thomas.wunschel@binastar.de>
     */
    private static class NamespaceCompatibilityFilter extends XMLFilterImpl {

        /** the namespace URL for persistence V2.1 */
        private static final String NAMESPACE_2_1 = "http://xmlns.jcp.org/xml/ns/persistence";

        @Override
        public void endElement(final String uri, final String localName, final String qName) throws SAXException {
            super.endElement(NAMESPACE_2_1, localName, qName);
        }

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes atts)
                throws SAXException {
            super.startElement(NAMESPACE_2_1, localName, qName, atts);
        }

    }

    /**
     * The Meta-Persistence header.
     */
    public static final String META_PERSISTENCE = "Meta-Persistence";

    /**
     * The logger.
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(JPAManager.class);

    /**
     * The bundle context, used to register the tracker.
     */
    @Context
    BundleContext context;

    /**
     * The factory used to create Persistence Unit 'instances'
     */
    @Requires(filter = "(factory.name=org.wisdom.framework.jpa.PersistenceUnitComponent)")
    Factory factory;

    /**
     * Set to be sure the weaving hook is registered first.
     */
    @Requires
    JPATransformer transformer;

    /**
     * The tracked bundle.
     */
    BundleTracker<PersistentBundle> bundles;
    
    /**
     * the JAXB context for unmarshalling the persistence.xml.
     */
    JAXBContext jcPersistence;


    @Validate
    void start() throws Exception {
        jcPersistence = JAXBContext.newInstance(Persistence.class);
        
        // Track bundles.
        bundles = new BundleTracker<PersistentBundle>(context, Bundle.ACTIVE + Bundle.STARTING, null) {

            /**
             * A new bundle arrives, check whether or not it contains persistence unit.
             * @param bundle the bundle
             * @param event the event
             * @return the Persistence Bundle object if the bundle contain PU, {@code null} if none (bundle not
             * tracked)
             */
            @Override
            public PersistentBundle addingBundle(Bundle bundle, BundleEvent event) {
                try {
                    // Parse any persistence units, returns null (not tracked) when there is no persistence unit
                    return parse(bundle);
                } catch (Exception e) {
                    LOGGER.error("While parsing bundle {} for a persistence unit we encountered " +
                                    "an unexpected exception {}. This bundle (also the other persistence " +
                                    "units in this bundle) will be ignored.",
                            bundle, e.getMessage(), e);
                    //noinspection Contract
                    return null;
                }
            }

            /**
             * A bundle is leaving.
             * @param bundle the bundle
             * @param event the event
             * @param pu the persistent bundle
             */
            @Override
            public void removedBundle(Bundle bundle, BundleEvent event, PersistentBundle pu) {
                pu.destroy();
            }
        };

        bundles.open();
    }

    /**
     * Closes the tracker.
     */
    @Invalidate
    void stop() {
        bundles.close();
    }

    /**
     * Check a bundle for persistence units following the rules in the OSGi
     * spec.
     * <p>
     * A Persistence Bundle is a bundle that specifies the Meta-Persistence
     * header, see Meta Persistence Header on page 439. This header refers to
     * one or more Persistence Descriptors in the Persistence Bundle. Commonly,
     * this is the META-INF/persistence.xml resource. This location is the
     * standard for non- OSGi environments, however an OSGi bundle can also use
     * other locations as well as multiple resources. Any entity classes must
     * originate in the bundle's JAR, it cannot come from a fragment. This
     * requirement is necessary to simplify enhancing entity classes.
     *
     * @param bundle the bundle to be searched
     * @return a Persistent Bundle or null if it has no matching persistence
     * units
     */
    PersistentBundle parse(Bundle bundle) throws Exception {
        LOGGER.debug("Analysing bundle {}", bundle.getBundleId());
        String metapersistence = bundle.getHeaders().get(META_PERSISTENCE);

        if (metapersistence == null || metapersistence.trim().isEmpty()) {
            // Check default location (except for system bundle)
            if (bundle.getBundleId() != 0 && bundle.getResource("META-INF/persistence.xml") != null) {
                // Found at the default location
                metapersistence = "META-INF/persistence.xml";
            } else {
                return null;
            }
        }
        LOGGER.info("META_PERSISTENCE header found in bundle {} : {}", bundle.getBundleId(), metapersistence);


        // We can have multiple persistence units.
        Set<Persistence.PersistenceUnit> set = new HashSet<>();
        for (String location : Splitter.on(",").omitEmptyStrings().trimResults().splitToList(metapersistence)) {
            LOGGER.info("Analysing location {}", location);
            // Lets remember where we came from
            Persistence.PersistenceUnit.Properties.Property p =
                    new Persistence.PersistenceUnit.Properties.Property();
            p.setName("location");
            p.setValue(location);

            // Try to find the resource for the persistence unit
            // on the classpath: getResource

            URL url = bundle.getResource(location);
            if (url == null) {
                LOGGER.error("Bundle {} specifies location '{}' in the Meta-Persistence header but no such" +
                        " resource is found in the bundle at that location.", bundle, location);
            } else {
                // Parse the XML file.
                Persistence persistence = unmarshalPersistence(url);
				
                LOGGER.info("Parsed persistence: {}, unit {}", persistence, persistence.getPersistenceUnit());
                for (Persistence.PersistenceUnit pu : persistence.getPersistenceUnit()) {
                    if (pu.getProperties() == null) {
                        pu.setProperties(new Persistence.PersistenceUnit.Properties());
                    }
                    pu.getProperties().getProperty().add(p);
                    set.add(pu);
                    LOGGER.info("Adding persistence unit {}", pu);
                }
            }
        }

        // Ignore this bundle if no valid PUs
        if (set.isEmpty()) {
            LOGGER.warn("No persistence unit found in bundle {}, despite a META_PERSISTENCE header ({})",
                    bundle.getBundleId(), metapersistence);
            return null;
        }

        return new PersistentBundle(bundle, set, factory);
    }

    /**
     * Unmarshals the persistence.xml at the given URL. The {@link Persistence} model is generated from
     * {@code persistence_2_1.xsd}, which by default only supports version 2.1. Support for version 1.0/2.0 has been
     * added by using the {@link NamespaceFilter} for xml parsing.
     * 
     * @param url
     *            the URL of the persistence.xml to unmarshal
     * @return the unmarshalled persistence.xml
     * @throws JAXBException
     *             if the URL could not be unmarshalled
     */
    private Persistence unmarshalPersistence(final URL url) throws JAXBException {
        try {
            final SAXParserFactory spf = SAXParserFactory.newInstance();
            final SAXParser sp = spf.newSAXParser();
            final XMLReader xr = sp.getXMLReader();
            final Unmarshaller unmarshaller = jcPersistence.createUnmarshaller();
            final UnmarshallerHandler unmarshallerHandler = unmarshaller.getUnmarshallerHandler();
            final InputSource xml = new InputSource(url.openStream());

            final XMLFilter filter = new NamespaceCompatibilityFilter();
            filter.setParent(xr);
            filter.setContentHandler(unmarshallerHandler);
            filter.parse(xml);

            return (Persistence) unmarshallerHandler.getResult();
        } catch (IllegalStateException | ParserConfigurationException | SAXException | IOException e) {
            // technically not a JAXBException, but close enough
            throw new JAXBException(e);
        }
    }
}
