package ch.so.agi.cadastre;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.jdbi.v3.core.Jdbi;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.Startup;

@Startup
@ApplicationScoped
public class WsConfig {

    @Inject
    AgroalDataSource dataSource;
    
//    private Jdbi jdbiInstance;

    // TODO
    // Verstehe es nicht...
    @ApplicationScoped
    @Produces
    public Jdbi jdbi() {
        System.out.println("create jdbi");
        return Jdbi.create(dataSource);
    }       
    
//    public synchronized Jdbi jdbi() {
//        if (jdbiInstance == null) {
//            System.out.println("create jdbi");
//            jdbiInstance = Jdbi.create(dataSource);
//            return jdbiInstance;
//        } else {
//            return jdbiInstance;
//        }
//    }
}
