package ch.so.agi.cadastre;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Context;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.resteasy.specimpl.RequestImpl;

@Path("/")
public class MainController {
    private static final Logger LOGGER = Logger.getLogger(MainController.class);

    private static final String PARAM_FORMAT_PDF = "pdf";
    private static final String PARAM_FORMAT_XML = "xml";

    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT = "dm01vch24lv95dliegenschaften_liegenschaft";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT = "dm01vch24lv95dliegenschaften_selbstrecht";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK = "dm01vch24lv95dliegenschaften_bergwerk";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK = "dm01vch24lv95dliegenschaften_grundstueck";
    private static final String TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LSNACHFUEHRUNG = "dm01vch24lv95dliegenschaften_lsnachfuehrung";
    private static final String TABLE_DM01VCH24LV95DGEMEINDEGRENZEN_GEMEINDE = "dm01vch24lv95dgemeindegrenzen_gemeinde";  
    private static final String TABLE_DM01VCH24LV95DBODENBEDECKUNG_BOFLAECHE  = "dm01vch24lv95dbodenbedeckung_boflaeche"; 
    private static final String TABLE_DM01VCH24LV95DBODENBEDECKUNG_PROJBOFLAECHE  = "dm01vch24lv95dbodenbedeckung_projboflaeche"; 
    private static final String TABLE_DM01VCH24LV95DBODENBEDECKUNG_GEBAEUDENUMMER = "dm01vch24lv95dbodenbedeckung_gebaeudenummer";
    private static final String TABLE_DM01VCH24LV95DBODENBEDECKUNG_PROJGEBAEUDENUMMER = "dm01vch24lv95dbodenbedeckung_projgebaeudenummer";
    private static final String TABLE_DM01VCH24LV95DEINZELOBJEKTE_EINZELOBJEKT  = "dm01vch24lv95deinzelobjekte_einzelobjekt"; 
    private static final String TABLE_DM01VCH24LV95DEINZELOBJEKTE_FLAECHENELEMENT  = "dm01vch24lv95deinzelobjekte_flaechenelement"; 
    private static final String TABLE_DM01VCH24LV95DEINZELOBJEKTE_OBJEKTNUMMER  = "dm01vch24lv95deinzelobjekte_objektnummer"; 
    private static final String TABLE_DM01VCH24LV95NOMENKLATUR_FLURNAME = "dm01vch24lv95dnomenklatur_flurname";
    private static final String TABLE_PLZOCH1LV95DPLZORTSCHAFT_PLZ6 = "plzoch1lv95dplzortschaft_plz6";
    private static final String TABLE_PLZOCH1LV95DPLZORTSCHAFT_ORTSCHAFT = "plzoch1lv95dplzortschaft_ortschaft";
    private static final String TABLE_PLZOCH1LV95DPLZORTSCHAFT_ORTSCHAFTSNAME = "plzoch1lv95dplzortschaft_ortschaftsname";
    private static final String TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_GEBAEUDEEINGANG = "dm01vch24lv95dgebaeudeadressen_gebaeudeeingang";
    private static final String TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATIONSNAME = "dm01vch24lv95dgebaeudeadressen_lokalisationsname"; 
    private static final String TABLE_SO_G_V_0180822GRUNDBUCHKREISE_GRUNDBUCHKREIS = "so_g_v_0180822grundbuchkreise_grundbuchkreis";
    private static final String TABLE_SO_G_V_0180822NACHFUEHRUNGSKREISE_GEMEINDE = "so_g_v_0180822nachfuehrngskrise_gemeinde";
    private static final String TABLE_OERB_XTNX_V1_0ANNEX_LOGO = "oerb_xtnx_v1_0annex_logo";

    protected static final String extractNS = "http://geo.so.ch/schema/AGI/Cadastre/0.9/Extract";
    private static final String LOGO_ENDPOINT = "logo";
    
    private static byte[] minimalImage=Base64.getDecoder().decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAACklEQVR4nGMAAQAABQABDQottAAAAABJRU5ErkJggg==");

    @ConfigProperty(name = "cadastre.dbschema", defaultValue="live") 
    String dbschema;
    
    @ConfigProperty(name = "cadastre.minIntersection", defaultValue="1.0") 
    double minIntersection;
    
    @ConfigProperty(name = "cadastre.tmpdir", defaultValue="/tmp") 
    String cadastreTmpdir;

    @GET
    @Path("ping")    
    @Produces(MediaType.TEXT_PLAIN)
    public String ping() {
        return "cadastre-web-service";
    }
    
    @GET
    @Path("fubar")
    @Produces(MediaType.TEXT_PLAIN)
    public Map<String, String> fubar(@Context HttpHeaders headers) {
        Map<String, String> resultMap = new HashMap<>();
        headers.getRequestHeaders().forEach(
                (key, values) -> resultMap.put(key, String.join(",", values)));
        return resultMap;
    }
    
    @GET
    @Path("fubar2")
    @Produces(MediaType.TEXT_PLAIN)
    public String fubar3(@Context UriInfo uriInfo) {
        
        LOGGER.info(uriInfo.getBaseUri());
        LOGGER.info(uriInfo.getPath());
        LOGGER.info(uriInfo.getPathParameters().toString());
        LOGGER.info(uriInfo.getRequestUri().toString());
        LOGGER.info(uriInfo.getPathSegments().toString());
        
        LOGGER.info("*"+this.getLogoRef(uriInfo.getBaseUriBuilder(), "logoId"));
        
        return "adf";
    }
    
    
    
//    @GET
//    @Path("logo/{id}")
//    public Response getLogo(@PathParam("id") String id) {
//        LOGGER.info("id " + id);
//        byte image[] = getImageOrNull(id);
//        if (image == null) {
//            return Response.noContent().build();
//        }
//        return Response.ok(image).header("content-disposition", "attachment; filename=" + id + ".png")
//                .header("content-length", image.length)
//                .header("content-type", "image/png")
//                .build();
//    }
    
    private String getSchema() {
        return dbschema!=null?dbschema:"xcadastre";
    }    
    
    private String getLogoRef(UriBuilder builder, String id) {
//        return ServletUriComponentsBuilder.fromCurrentContextPath().pathSegment(LOGO_ENDPOINT).pathSegment(id).build().toUriString();
        
//        uriInfo.getBaseUriBuilder().fromPath(LOGO_ENDPOINT).fromPath(id).build(values).
//        UriBuilder builder = UriBuilder.fromPath(LOGO_ENDPOINT+"/"+id);
//        builder.p
        return builder.fromPath(LOGO_ENDPOINT).fromPath(id).build().getHost();
    }
    
//    private byte[] getImage(String code) {
//        byte[] ret = getImageOrNull(code);
//        if (ret != null) {
//            return ret;
//        }
//        return minimalImage;
//    }
//    
//    private byte[] getImageOrNull(String code) {
//        java.util.List<byte[]> baseData = jdbcTemplate.queryForList(
//                "SELECT logo FROM " + getSchema() + "." + TABLE_OERB_XTNX_V1_0ANNEX_LOGO + " WHERE acode=?",
//                byte[].class, code);
//        if (baseData != null && baseData.size() == 1) {
//            return baseData.get(0);
//        }
//        return null;
//    }
}
