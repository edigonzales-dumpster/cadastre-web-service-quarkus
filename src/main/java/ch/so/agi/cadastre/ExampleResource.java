package ch.so.agi.cadastre;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultIterable;

@Path("/hello")
public class ExampleResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        
        Jdbi jdbi = Jdbi.create("jdbc:postgresql://localhost:54321/grundstuecksinformation", "gretl", "gretl");
        try (Handle handle = jdbi.open()) {
            ResultIterable<String> foo = handle.createQuery("SELECT aname FROM live.dm01vch24lv95dgemeindegrenzen_gemeinde").mapTo(String.class);
            foo.forEach(r -> {
                System.out.println(r);
            });
        }
        
        
        
        
        return "hello";
    }
}