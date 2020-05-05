package ch.so.agi.cadastre;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.jdbi.v3.core.Jdbi;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.Startup;

@Startup
@ApplicationScoped
public class WsConfig {

    @Inject
    AgroalDataSource dataSource;
    
    // TODO
    // Verstehe es nicht...
    @ApplicationScoped
    @Produces
    public Jdbi jdbi() {
        System.out.println("create jdbi");
        return Jdbi.create(dataSource);
    }       
    
    @ApplicationScoped
    @Produces
    public Marshaller createMarshaller() throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance( ch.so.geo.schema.agi.cadastre._0_9.extract.ObjectFactory.class );
        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
        return jaxbMarshaller;
    }
}
