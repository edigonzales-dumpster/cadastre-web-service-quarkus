package ch.so.agi.cadastre;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
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
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import ch.so.geo.schema.agi.cadastre._0_9.extract.GetEGRIDResponse;
import io.agroal.api.AgroalDataSource;

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
    
    @Inject
    WsConfig wsConfig;
    
    private Jdbi jdbi;
    
    @PostConstruct
    public void init() {
        this.jdbi = wsConfig.jdbi();
    }


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
    @Path(value = "getegrid/{format}")
    @Produces({MediaType.APPLICATION_XML})
    public Response getEgridByXY(@PathParam("format") String format, @QueryParam("XY") String xy, @QueryParam("GNSS") String gnss) {
        if (!format.equals(PARAM_FORMAT_XML)) {
            throw new IllegalArgumentException("unsupported format <" + format + ">");
        }
        
        if (xy == null && gnss == null) {
            throw new IllegalArgumentException("parameter XY or GNSS required");
        } else if (xy != null && gnss != null) {
            throw new IllegalArgumentException("only one of parameters XY or GNSS is allowed");
        }
        Coordinate coord = null;
        int srid = 2056;
        double scale = 1000.0;
        if (xy != null) {
            coord = parseCoord(xy);
            srid = 2056;
            if (coord.x < 2000000.0) {
                srid = 21781;
            }
        } else {
            coord = parseCoord(gnss);
            srid = 4326;
            scale = 100000.0;
        }
        
        WKBWriter geomEncoder = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN, true);
        PrecisionModel precisionModel = new PrecisionModel(scale);
        GeometryFactory geomFact = new GeometryFactory(precisionModel, srid);
        byte geom[] = geomEncoder.write(geomFact.createPoint(coord));

        GetEGRIDResponse ret = new GetEGRIDResponse();
        
        
        LOGGER.info("fubar");
        
        return Response.noContent().build();
    }

    
    @GET
    @Path(value = "extract/{format}/geometry/{egrid}") 
    @Produces({MediaType.APPLICATION_XML, "application/pdf"})
    public void getExtractWithGeometryByEgrid(@Context UriInfo uriInfo, @PathParam("format") String format, @PathParam("egrid") String egrid) {
        LOGGER.info(uriInfo.getBaseUri());
        LOGGER.info(uriInfo.getPath());
        LOGGER.info(uriInfo.getPathParameters().toString());
        LOGGER.info(uriInfo.getRequestUri().toString());
        LOGGER.info(uriInfo.getPathSegments().toString());
        LOGGER.info("***"+uriInfo.getRequestUri().toString().replace(uriInfo.getPath(), ""));
        LOGGER.info(getLogoRef(uriInfo.getRequestUri().toString().replace(uriInfo.getPath(), ""), "myLogo"));
        LOGGER.info(this.getImageOrNull("ch.so"));
        
    }
    
    @GET
    @Path("logo/{id}")
    public Response getLogo(@PathParam("id") String id) {
        LOGGER.info("id " + id);
        byte image[] = getImageOrNull(id);
        if (image == null) {
            return Response.noContent().build();
        }
        return Response.ok(image).header("content-disposition", "attachment; filename=" + id + ".png")
                .header("content-length", image.length)
                .header("content-type", "image/png")
                .build();
    }    
    
    // TODO 
    // Es gibt keinen Applikationscontext wie bei Spring Boot (resp.
    // Tomcat. Gibt es bessere Lösungen? Nicht getested, ob 
    // bei x-forwarded header etc. dies so auch noch funktioniert.
    private String getLogoRef(String applicationUrl, String id) {
        return applicationUrl + "/" + LOGO_ENDPOINT + "/" + id;
    }
    
    private String getSchema() {
        return dbschema!=null?dbschema:"xcadastre";
    }    
    
    private byte[] getImage(String code) {
        byte[] ret = getImageOrNull(code);
        if (ret != null) {
            return ret;
        }
        return minimalImage;
    }
    
    private byte[] getImageOrNull(String code) {
        try (Handle handle = jdbi.open()) {
            byte[] baseData = handle.select("SELECT logo FROM " + getSchema() + "." + TABLE_OERB_XTNX_V1_0ANNEX_LOGO + " WHERE acode=?", code)
                    .mapTo(byte[].class)
                    .one();
            if (baseData != null) {
                return baseData;
            }
        }
        return null;
    }
    
    private Coordinate parseCoord(String xy) {
        int sepPos = xy.indexOf(',');
        double x = Double.parseDouble(xy.substring(0, sepPos));
        double y = Double.parseDouble(xy.substring(sepPos + 1));
        Coordinate coord = new Coordinate(x, y);
        return coord;
    }    
}
