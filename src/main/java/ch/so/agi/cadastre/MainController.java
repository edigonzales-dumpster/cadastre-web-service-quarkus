package ch.so.agi.cadastre;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;
import javax.ws.rs.core.Context;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.resteasy.specimpl.RequestImpl;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import ch.so.geo.schema.agi.cadastre._0_9.extract.AddressType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.BuildingType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.BuildingEntryType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.Extract;
import ch.so.geo.schema.agi.cadastre._0_9.extract.GetEGRIDResponse;
import ch.so.geo.schema.agi.cadastre._0_9.extract.GetExtractByIdResponse;
import ch.so.geo.schema.agi.cadastre._0_9.extract.LCType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.LandCoverShareType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.LocalNameType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.OrganisationType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.PersonAddressType;
import ch.so.geo.schema.agi.cadastre._0_9.extract.RealEstateDPR;
import ch.so.geo.schema.agi.cadastre._0_9.extract.RealEstateType;
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
    
    @Inject
    Marshaller marshaller;
    
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
        
        List<JAXBElement<?>[]> gsList;
        try (Handle handle = jdbi.open()) { 
            gsList = handle.select("SELECT egris_egrid,nummer,g.nbident,g.art,TO_CHAR(nf.gueltigereintrag, 'yyyy-mm-dd') gueltigereintrag,TO_CHAR(nf.gbeintrag, 'yyyy-mm-dd') gbeintrag,ST_AsText(geometrie) FROM " +getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LSNACHFUEHRUNG + " nf"
                    + " LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g ON g.entstehung = nf.t_id"
                    +" LEFT JOIN (SELECT liegenschaft_von as von, geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT
                     +" UNION ALL SELECT selbstrecht_von as von,  geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT
                     +" UNION ALL SELECT bergwerk_von as von,     geometrie FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+") b ON b.von=g.t_id WHERE ST_DWithin(ST_Transform(?,2056),b.geometrie,1.0)", geom)
                .map((rs, ctx) -> {                    
                    JAXBElement<?> ele[]=new JAXBElement[6];
                    ele[0]=new JAXBElement<String>(new QName(extractNS,"Egrid"),String.class,rs.getString(1));
                    ele[1]=new JAXBElement<String>(new QName(extractNS,"Number"),String.class,rs.getString(2));
                    ele[2]=new JAXBElement<String>(new QName(extractNS,"IdentND"),String.class,rs.getString(3));
                    ele[3]=new JAXBElement<RealEstateType>(new QName(extractNS,"Type"),RealEstateType.class,gsArtLookUp(rs.getString(4)));
                    String sqlDate = (rs.getString(6) != null) ? rs.getString(6) : rs.getString(5);
                    ele[4]=new JAXBElement<XMLGregorianCalendar>(new QName(extractNS,"StateOf"),XMLGregorianCalendar.class,stringDateToXmlGregorianCalendar(sqlDate));
                    ele[5]=new JAXBElement<String>(new QName(extractNS,"Limit"),String.class,rs.getString(7));
                    return ele;
                }).list();
        }
        
        for (JAXBElement<?>[] gs : gsList) {
            ret.getEgridsAndLimitsAndStateOves().add(gs[0]);
            ret.getEgridsAndLimitsAndStateOves().add(gs[1]);
            ret.getEgridsAndLimitsAndStateOves().add(gs[2]);
            ret.getEgridsAndLimitsAndStateOves().add(gs[3]);            
            ret.getEgridsAndLimitsAndStateOves().add(gs[4]);
            ret.getEgridsAndLimitsAndStateOves().add(gs[5]);
        }
        
        if (gsList.size() > 0) {
            return Response.ok(ret).build();
        } else {
            return Response.noContent().build();
        }
    }
    
    @GET
    @Path(value = "extract/{format}/geometry/{egrid}") 
    @Produces({MediaType.APPLICATION_XML, "application/pdf"})
    public Response getExtractWithGeometryByEgrid(@Context UriInfo uriInfo, @PathParam("format") String format, @PathParam("egrid") String egrid, @QueryParam("WITHIMAGES") String withImagesParam) {
        if (!format.equals(PARAM_FORMAT_XML) && !format.equals(PARAM_FORMAT_PDF)) {
            throw new IllegalArgumentException("unsupported format <" + format + ">");
        }

        boolean withImages = withImagesParam==null?false:true;
        if (format.equals(PARAM_FORMAT_PDF)) {
            withImages = true;
        }  
        
        Grundstueck parcel = getParcelByEgrid(egrid);
        if (parcel == null) {
            return Response.noContent().build();
        }

        Extract extract = new Extract();
        
        XMLGregorianCalendar today = null;
        try {
            LOGGER.debug("timezone id " + TimeZone.getDefault().getID());
            GregorianCalendar gdate = new GregorianCalendar();
            gdate.setTime(new java.util.Date());
            today = DatatypeFactory.newInstance().newXMLGregorianCalendar(gdate);
        } catch (DatatypeConfigurationException e) {
            throw new IllegalStateException(e);
        }

        extract.setCreationDate(today);
        
        if (withImages) {
            try {            
                ClassLoader classLoader = getClass().getClassLoader();
                File file = new File(classLoader.getResource("/META-INF/resources/logo-grundstuecksinformation_no_alpha.png").getFile());
                InputStream inputStream = new FileInputStream(file);
                byte[] bdata = inputStream.readAllBytes();
                extract.setLogoGrundstuecksinformation(bdata);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            extract.setCantonalLogo(getImage("ch." + parcel.getNbident().substring(0, 2).toLowerCase()));
            extract.setMunicipalityLogo(getImage("ch." + String.valueOf(parcel.getBfsnr())));
        } else {
            String applicationContext = this.getApplicationContext(uriInfo);
            extract.setLogoGrundstuecksinformationRef(applicationContext + "/" + "logo-grundstuecksinformation_no_alpha.png"); 
            extract.setCantonalLogoRef(getLogoRef(applicationContext, "ch." + parcel.getNbident().substring(0, 2).toLowerCase()));
            extract.setMunicipalityLogoRef(getLogoRef(applicationContext, "ch." + String.valueOf(parcel.getBfsnr())));
        }
        
        OrganisationType responsibleOffice = new OrganisationType();
        responsibleOffice.setName("Amt für Geoinformation");
        responsibleOffice.setEmail("agi@bd.so.ch");
        responsibleOffice.setWeb("https://agi.so.ch");
        responsibleOffice.setPhone("032 627 75 92");
        AddressType responsibleOfficeAddress = new AddressType();
        responsibleOfficeAddress.setStreet("Rötistrasse");
        responsibleOfficeAddress.setNumber("4");
        responsibleOfficeAddress.setPostalCode(Integer.valueOf("4500"));
        responsibleOfficeAddress.setCity("Solothurn");
        responsibleOffice.setAddress(responsibleOfficeAddress);
        extract.setResponsibleOffice(responsibleOffice);
        
        RealEstateDPR realEstate = new RealEstateDPR();
        realEstate.setEGRID(egrid);
        realEstate.setIdentND(parcel.getNbident());
        realEstate.setNumber(parcel.getNummer());
        realEstate.setSubunitOfLandRegister(parcel.getGbSubKreis());
        realEstate.setMunicipality(parcel.getGemeinde());
        realEstate.setLimit(parcel.getGeometrie().toString());
        realEstate.setLandRegistryArea(new Double(parcel.getFlaechenmass()).intValue());
        realEstate.setType(gsArtLookUp(parcel.getArt()).value());

        setFlurnamen(realEstate, parcel.getGeometrie());
        setBodenbedeckung(realEstate, parcel.getGeometrie());
        setGebaeude(realEstate, parcel.getGeometrie());
        setNfGeometerAddress(realEstate, parcel.getBfsnr());
        setGrundbuchamtAddress(realEstate, parcel.getGbSubKreisNummer());
        setVermessungsaufsichtAddress(realEstate);
        
        extract.setRealEstate(realEstate);        
        GetExtractByIdResponse response = new GetExtractByIdResponse();
        response.setExtract(extract);
        
        if(format.equals(PARAM_FORMAT_PDF)) {
            return getExtractAsPdf(parcel, response);
        }
        return Response.ok(response).build();        
    }
    
    private Response getExtractAsPdf(Grundstueck parcel, GetExtractByIdResponse response) {
        File tmpFolder = new File(cadastreTmpdir,"cadastrews"+Thread.currentThread().getId());
        if(!tmpFolder.exists()) {
            tmpFolder.mkdirs();
        }
        LOGGER.info("tmpFolder {}" + tmpFolder.getAbsolutePath());

        File tmpExtractFile = new java.io.File(tmpFolder,parcel.getEgrid()+".xml");
        try {
            marshaller.marshal(response, new javax.xml.transform.stream.StreamResult(tmpExtractFile));

            
        } catch (JAXBException e) {
            throw new IllegalStateException(e.getMessage());
        }

        
        return null;
    }
    
    private void setVermessungsaufsichtAddress(RealEstateDPR realEstate) {       
        OrganisationType organisation = new OrganisationType();
        organisation.setName("Amt für Geoinformation");
        organisation.setEmail("agi@bd.so.ch");
        organisation.setWeb("https://agi.so.ch");
        organisation.setPhone("032 627 75 92");
        AddressType address = new AddressType();
        address.setStreet("Rötistrasse");
        address.setNumber("4");
        address.setPostalCode(Integer.valueOf("4500"));
        address.setCity("Solothurn");
        organisation.setAddress(address);
        realEstate.setSupervisionOffice(organisation);
    }
    
    private void setGrundbuchamtAddress(RealEstateDPR realEstate, int fosnr) {
        String stmt = "SELECT amt, amtschreiberei, strasse, hausnummer, plz, ortschaft, telefon, email, web, bfsnr FROM "+getSchema()+"."+TABLE_SO_G_V_0180822GRUNDBUCHKREISE_GRUNDBUCHKREIS
                + " WHERE grundbuchkreis_bfsnr = ?";
        try (Handle handle = jdbi.open()) {
            Map<String,Object> organisationMap = handle.select(stmt, fosnr)
                    .mapToMap()
                    .one();
            OrganisationType organisation = new OrganisationType();
            organisation.setName((String) organisationMap.get("amtschreiberei")); 
            if (organisationMap.get("amt") != null) organisation.setLine1((String) organisationMap.get("amt"));
            AddressType address = new AddressType();
            address.setStreet((String) organisationMap.get("strasse"));
            if(organisationMap.get("hausnummer") != null) address.setNumber((String) organisationMap.get("hausnummer"));
            address.setPostalCode((Integer) organisationMap.get("plz"));
            address.setCity((String) organisationMap.get("ortschaft"));
            organisation.setAddress(address);
            organisation.setPhone((String) organisationMap.get("telefon"));
            organisation.setEmail((String) organisationMap.get("email"));
            organisation.setWeb((String)organisationMap.get("web"));
            realEstate.setLandRegisterOffice(organisation);            
        }
    }
    
    private void setNfGeometerAddress(RealEstateDPR realEstate, int fosnr) {
        String stmt = "SELECT nfg_titel, nfg_name, nfg_vorname, firma, firma_zusatz, strasse, hausnummer, plz, ortschaft, telefon, web, email FROM "+getSchema()+"."+TABLE_SO_G_V_0180822NACHFUEHRUNGSKREISE_GEMEINDE
                + " WHERE bfsnr = ?";
        try (Handle handle = jdbi.open()) {
            Map<String,Object> organisationMap = handle.select(stmt, fosnr)
                .mapToMap()
                .one();
            OrganisationType organisation = new OrganisationType();
            organisation.setName((String) organisationMap.get("firma")); 
            if (organisationMap.get("firma_zusatz") != null) organisation.setLine1((String) organisationMap.get("firma_zusatz"));
            PersonAddressType person = new PersonAddressType();
            person.setLastName((String) organisationMap.get("nfg_name"));
            person.setFirstName((String) organisationMap.get("nfg_vorname"));
            organisation.setPerson(person);
            AddressType address = new AddressType();
            address.setStreet((String) organisationMap.get("strasse"));
            address.setNumber((String) organisationMap.get("hausnummer"));
            address.setPostalCode((Integer) organisationMap.get("plz"));
            address.setCity((String) organisationMap.get("ortschaft"));
            organisation.setAddress(address);
            organisation.setPhone((String) organisationMap.get("telefon"));
            organisation.setEmail((String) organisationMap.get("email"));
            organisation.setWeb((String)organisationMap.get("web"));
            realEstate.setSurveyorOffice(organisation);
        }
    }
    
    private void setGebaeude(RealEstateDPR realEstate, Geometry parcelGeom) {
        WKBWriter geomEncoder = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN);
        byte parcelWkbGeometry[] = geomEncoder.write(parcelGeom);

        PrecisionModel precisionModel = new PrecisionModel(1000.0);
        GeometryFactory geomFactory = new GeometryFactory(precisionModel);
        WKBReader decoder=new WKBReader(geomFactory);

        String stmt = "SELECT DISTINCT ON (bb.t_id) bb.t_id, ST_AsBinary(bb.geometrie) as geometrie, gwr_egid, 'realisiert' AS status, bb.art \n" + 
                "FROM \n" +
                "     "+getSchema()+"."+TABLE_DM01VCH24LV95DBODENBEDECKUNG_BOFLAECHE+" AS bb \n" + 
                "     LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DBODENBEDECKUNG_GEBAEUDENUMMER+" AS bbnr ON bbnr.gebaeudenummer_von = bb.t_id\n" + 
                "WHERE art = 'Gebaeude' AND ST_DWithin(ST_GeomFromWKB(:geom,2056), bb.geometrie, 0.1) \n" +
                "UNION ALL \n" +
                "SELECT DISTINCT ON (bb.t_id) bb.t_id, ST_AsBinary(bb.geometrie) as geometrie, gwr_egid, 'projektiert' AS status, bb.art \n" + 
                "FROM \n" +
                "     "+getSchema()+"."+TABLE_DM01VCH24LV95DBODENBEDECKUNG_PROJBOFLAECHE+" AS bb \n" + 
                "     LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DBODENBEDECKUNG_PROJGEBAEUDENUMMER+" AS bbnr ON bbnr.projgebaeudenummer_von = bb.t_id\n" + 
                "WHERE art = 'Gebaeude' AND ST_DWithin(ST_GeomFromWKB(:geom,2056), bb.geometrie, 0.1) \n" + 
                "UNION ALL \n" +
                "SELECT DISTINCT ON (fl.t_id) fl.t_id, ST_AsBinary(fl.geometrie) AS geometrie, eonr.gwr_egid, 'realisiert' AS status, eo.art \n" + 
                "FROM \n" + 
                "    (SELECT * FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DEINZELOBJEKTE_EINZELOBJEKT+" WHERE art = 'unterirdisches_Gebaeude') AS eo \n" + 
                "    JOIN (SELECT * FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DEINZELOBJEKTE_FLAECHENELEMENT+" WHERE ST_DWithin(ST_GeomFromWKB(:geom,2056), geometrie, 0.1)) AS fl \n" + 
                "    ON fl.flaechenelement_von = eo.t_id \n" + 
                "    LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DEINZELOBJEKTE_OBJEKTNUMMER+" AS eonr\n" + 
                "    ON eonr.objektnummer_von = eo.t_id";

        List<BuildingType> gebaeudeList;
        try (Handle handle = jdbi.open()) {
            gebaeudeList = handle.select(stmt)
                    .bind("geom", parcelWkbGeometry)
                    .map((rs, ctx) -> {
                        LOGGER.debug("bb t_id: " + rs.getString("t_id"));

                        Geometry gebaeudeGeometry = null;
                        try {
                            gebaeudeGeometry = decoder.read(rs.getBytes("geometrie"));
                        }  catch (org.locationtech.jts.io.ParseException e) {
                            throw new IllegalStateException(e);
                        }
                        String bb_egid = rs.getString("gwr_egid");
                        String status = rs.getString("status");
                        String art = rs.getString("art");

                        BuildingType gebaeude = new BuildingType();
                        if (status.equalsIgnoreCase("realisiert")) {
                            gebaeude.setPlanned(false);
                        } else {
                            gebaeude.setPlanned(true);
                        }
                        if (art.equalsIgnoreCase("Gebaeude")) {
                            gebaeude.setUndergroundStructure(false);
                        } else {
                            gebaeude.setUndergroundStructure(true);
                        }
                        if (bb_egid != null) gebaeude.setEgid(Integer.valueOf(bb_egid));
                        
                        Geometry intersection = null;
                        intersection = parcelGeom.intersection(gebaeudeGeometry);
                        LOGGER.debug(intersection.toString());
                        LOGGER.debug("intersection.getArea() {}" + intersection.getArea());
                        
                        double intersectionArea = intersection.getArea();
                        double gebaeudeArea = gebaeudeGeometry.getArea();
                        LOGGER.debug("intersectionArea {}" + intersectionArea);
                        LOGGER.debug("gebaeudeArea {}" + gebaeudeArea);

                        // Ignore building if it is less than minIntersection on the parcel.
                        if (intersection.isEmpty() || intersectionArea < minIntersection) {
                            return null;
                        }

                        // Falls der Unterschied zwischen dem Gebäude-Grundstück-Verschnitt und 
                        // dem gesamten Gebäude kleiner als minIntersection ist, ist das Gebäude
                        // vollständig auf dem Grundstück.
                        if (Math.abs(intersectionArea - gebaeudeArea) < minIntersection) {
                            gebaeude.setArea(gebaeudeArea);
                        } else {
                            gebaeude.setAreaShare(intersectionArea);
                        }
                        
                        byte gebaeudeWkbGeometry[] = geomEncoder.write(gebaeudeGeometry);

                        String stmtEntry = "SELECT ge.t_id, lokname.atext AS strassenname, ge.hausnummer, plz.plz, ortname.atext AS ortschaft, ge.astatus, ge.lage, ge.gwr_egid AS geb_egid, ge.gwr_edid \n" +
                                "FROM \n" +
                                "    "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_GEBAEUDEEINGANG+" AS ge \n" + 
                                "    LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DGEBAEUDEADRESSEN_LOKALISATIONSNAME+" AS lokname \n" + 
                                "    ON ge.gebaeudeeingang_von = lokname.benannte \n" + 
                                "    LEFT JOIN "+getSchema()+"."+TABLE_PLZOCH1LV95DPLZORTSCHAFT_ORTSCHAFT+" AS ort \n" + 
                                "    ON ST_Intersects(ge.lage, ort.flaeche) \n" + 
                                "    LEFT JOIN "+getSchema()+"."+TABLE_PLZOCH1LV95DPLZORTSCHAFT_ORTSCHAFTSNAME+" AS ortname \n" + 
                                "    ON ortname.ortschaftsname_von = ort.t_id \n" +
                                "    LEFT JOIN "+getSchema()+"."+TABLE_PLZOCH1LV95DPLZORTSCHAFT_PLZ6+" AS plz \n" + 
                                "    ON ST_Intersects(ge.lage, plz.flaeche ) \n" + 
                                "WHERE ge.istoffiziellebezeichnung = 'ja' AND ge.astatus = :status AND ge.im_gebaeude = :im_gebaeude AND ST_Intersects(ge.lage, ST_GeomFromWKB(:gebaeudeGeom,2056))";

                        
                        Map<String, Object> parameters = new HashMap<String, Object>();
                        parameters.put("gebaeudeGeom", gebaeudeWkbGeometry);
                        
                        if (status.equalsIgnoreCase("realisiert")) {
                            parameters.put("status", "real");
                        } else {
                            parameters.put("status", "projektiert");
                        }
                        
                        if (art.equalsIgnoreCase("Gebaeude")) {
                            parameters.put("im_gebaeude", "BB");
                        } else {
                            parameters.put("im_gebaeude", "EO");
                        }
                        
                        List<BuildingEntryType> buildingEntryList = handle.select(stmtEntry)
                                .bindMap(parameters)
                                .map((rsEntry, ctxEntry) -> {
                                    String strassenname = rsEntry.getString("strassenname");
                                    String hausnummer = rsEntry.getString("hausnummer");
                                    String plz = rsEntry.getString("plz");
                                    String ortschaft = rsEntry.getString("ortschaft");
                                    String geb_egid = rsEntry.getString("geb_egid");
                                    String gwr_edid = rsEntry.getString("gwr_edid");
                                    
                                    // TODO: Soll geprüft werden, ob der Eingang auf dem Grundstück liegt?
                                    // Kann entweder hier gemacht werden oder bereits in der Query.
                                    BuildingEntryType gebaeudeeingang = new BuildingEntryType();
                                    AddressType postalAddress = new AddressType();
                                    postalAddress.setStreet(strassenname);
                                    postalAddress.setNumber(hausnummer);
                                    postalAddress.setPostalCode(Integer.valueOf(plz));
                                    postalAddress.setCity(ortschaft);
                                    gebaeudeeingang.setPostalAddress(postalAddress);
                                    if (geb_egid != null) gebaeudeeingang.setEgid(Integer.valueOf(geb_egid));
                                    if (gwr_edid != null) gebaeudeeingang.setEdid(Integer.valueOf(gwr_edid));
                                    
                                    return gebaeudeeingang;
                                })
                                .list();
                        gebaeude.getBuildingEntries().addAll(buildingEntryList);
                        return gebaeude;
                    })
                    .list();
        }
        realEstate.getBuildings().addAll(gebaeudeList);
    }
    
    private void setBodenbedeckung(RealEstateDPR realEstate, Geometry geometry) {
        WKBWriter geomEncoder = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN);
        byte wkbGeometry[] = geomEncoder.write(geometry);
        
        List<LandCoverShareType> bbList;
        try (Handle handle = jdbi.open()) { 
            bbList = handle.select("SELECT ST_Area(ST_Union(geom)) AS flaechenmass, art \n" + 
                    "FROM (SELECT (ST_Dump(ST_CollectionExtract(ST_Intersection(ST_GeomFromWKB(:geom,2056), b.geometrie), 3))).geom AS geom, b.art FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DBODENBEDECKUNG_BOFLAECHE+" AS b WHERE ST_Intersects(ST_GeomFromWKB(:geom,2056), b.geometrie)) AS foo \n" + 
                    "WHERE ST_IsValid(geom) IS TRUE AND geom IS NOT NULL GROUP BY art")
                    .bind("geom", wkbGeometry)
                    .map((rs, ctx) -> {
                        double flaechenmass = rs.getDouble("flaechenmass");
                        String art = rs.getString("art");
                        LandCoverShareType bb = new LandCoverShareType(); 
                        bb.setType(LCType.fromValue(art));
                        bb.setTypeDescription(LCType.fromValue(art).value().substring(LCType.fromValue(art).value().lastIndexOf(".") + 1).trim());
                        bb.setArea(flaechenmass);
                        return bb;
                    })
                    .list();
        }
        realEstate.getLandCoverShares().addAll(bbList);
    }

    private void setFlurnamen(RealEstateDPR realEstate, Geometry geometry) {
        WKBWriter geomEncoder = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN);
        byte wkbGeometry[] = geomEncoder.write(geometry);

        List<LocalNameType> localNameList;
        try (Handle handle = jdbi.open()) { 
            localNameList = handle.select("SELECT aname as flurname \n" + 
                    "FROM (SELECT (ST_Dump(ST_CollectionExtract(ST_Intersection(ST_GeomFromWKB(:geom,2056), f.geometrie), 3))).geom AS geom, f.aname FROM "+getSchema()+"."+TABLE_DM01VCH24LV95NOMENKLATUR_FLURNAME+" AS f WHERE ST_Intersects(ST_GeomFromWKB(:geom,2056), f.geometrie)) AS foo \n" + 
                    "WHERE ST_IsValid(geom) IS TRUE AND geom IS NOT NULL GROUP BY aname")
                    .bind("geom", wkbGeometry)
                    .map((rs, ctx) -> {
                        String name = rs.getString("flurname");
                        LocalNameType localName = new LocalNameType(); 
                        localName.setName(name);
                        return localName;
                    })
                    .list();
        }
        realEstate.getLocalNames().addAll(localNameList);
    }

    
    private Grundstueck getParcelByEgrid(String egrid) {
        PrecisionModel precisionModel = new PrecisionModel(1000.0);
        GeometryFactory geomFactory = new GeometryFactory(precisionModel);
        WKBReader decoder=new WKBReader(geomFactory);

        List<Grundstueck> gslist;
        try (Handle handle = jdbi.open()) { 
            gslist = handle.select("SELECT ST_AsBinary(l.geometrie) as l_geometrie,ST_AsBinary(s.geometrie) as s_geometrie,ST_AsBinary(b.geometrie) as b_geometrie,nummer,nbident,art,gesamteflaechenmass,l.flaechenmass as l_flaechenmass,s.flaechenmass as s_flaechenmass,b.flaechenmass as b_flaechenmass FROM "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_GRUNDSTUECK+" g"
                    +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_LIEGENSCHAFT+" l ON g.t_id=l.liegenschaft_von "
                    +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_SELBSTRECHT+" s ON g.t_id=s.selbstrecht_von"
                    +" LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DLIEGENSCHAFTEN_BERGWERK+" b ON g.t_id=b.bergwerk_von"
                    +" WHERE g.egris_egrid=?", egrid)
                .map((rs, ctx) -> {       
                    Geometry polygon=null;
                    byte l_geometrie[]=rs.getBytes("l_geometrie");
                    byte s_geometrie[]=rs.getBytes("s_geometrie");
                    byte b_geometrie[]=rs.getBytes("b_geometrie");

                    try {
                        if(l_geometrie!=null) {
                            polygon=decoder.read(l_geometrie);
                        } else if(s_geometrie!=null) {
                            polygon=decoder.read(s_geometrie);
                        } else if(b_geometrie!=null) {
                            polygon=decoder.read(b_geometrie);
                        } else {
                            throw new IllegalStateException("no geometrie");
                        }
                        if(polygon==null || polygon.isEmpty()) {
                            return null;
                        }
                    } catch (org.locationtech.jts.io.ParseException e) {
                        throw new IllegalStateException(e);
                    }

                    Grundstueck ret=new Grundstueck();
                    ret.setGeometrie(polygon);
                    ret.setEgrid(egrid);
                    ret.setNbident(rs.getString("nbident"));
                    ret.setNummer(rs.getString("nummer"));
                    ret.setArt(rs.getString("art"));
                    int f = rs.getInt("gesamteflaechenmass");
                    if(rs.wasNull()) {
                        if (l_geometrie!=null) {
                            f=rs.getInt("l_flaechenmass");
                        } else if(s_geometrie!=null) {
                            f=rs.getInt("s_flaechenmass");
                        } else if(b_geometrie!=null) {
                            f=rs.getInt("b_flaechenmass");
                        } else {
                            throw new IllegalStateException("no geometrie");
                        }
                    }
                    ret.setFlaechenmass(f);
                    ret.setKanton(ret.getNbident().substring(0,2).toUpperCase());
                    return ret;
                }).list();
        }
        
        if (gslist==null || gslist.isEmpty()) {
            return null;
        }
        
        Polygon polygons[] = new Polygon[gslist.size()];
        int i=0;
        for (Grundstueck gs : gslist) {
            polygons[i++] = (Polygon)gs.getGeometrie();
        }
        Geometry multiPolygon=geomFactory.createMultiPolygon(polygons);
        Grundstueck gs = gslist.get(0);
        gs.setGeometrie(multiPolygon);

        // Grundbuchkreis 
        try (Handle handle = jdbi.open()) {
            Map<String,Object> gbKreis = handle.select("SELECT gb.aname,gb.grundbuchkreis_bfsnr,gb.bfsnr,gem.aname AS gemeindename FROM "+getSchema()+"."+TABLE_SO_G_V_0180822GRUNDBUCHKREISE_GRUNDBUCHKREIS+" AS gb" +
                    " LEFT JOIN "+getSchema()+"."+TABLE_DM01VCH24LV95DGEMEINDEGRENZEN_GEMEINDE+" AS gem ON gem.bfsnr = gb.bfsnr" +
                    " WHERE nbident=?", gs.getNbident())
                .mapToMap()
                .one();
            gs.setGbSubKreis((String) gbKreis.get("aname"));
            gs.setGbSubKreisNummer((int) gbKreis.get("grundbuchkreis_bfsnr"));
            gs.setBfsnr((Integer) gbKreis.get("bfsnr"));
            gs.setGemeinde((String) gbKreis.get("gemeindename"));            
        }
        return gs;
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
    private String getLogoRef(String applicationContext, String id) {
        return applicationContext + "/" + LOGO_ENDPOINT + "/" + id;
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
    
    private XMLGregorianCalendar stringDateToXmlGregorianCalendar(String sqlDate) {
        XMLGregorianCalendar stateOf = null;
        GregorianCalendar gdate = new GregorianCalendar();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = formatter.parse(sqlDate);                            
            gdate.setTime(date);
            stateOf = DatatypeFactory.newInstance().newXMLGregorianCalendar(gdate);
            return stateOf;
        } catch (ParseException | DatatypeConfigurationException e ) {
            e.printStackTrace();                            
            LOGGER.error(e.getMessage());
            throw new IllegalStateException(e);
        } 
    }
    
    private RealEstateType gsArtLookUp(String gsArt) {
        if("Liegenschaft".equals(gsArt)) {
            return RealEstateType.REAL_ESTATE;
        } else if("SelbstRecht.Baurecht".equals(gsArt)) {
            return RealEstateType.DISTINCT_AND_PERMANENT_RIGHTS_BUILDING_RIGHT;
        } else if("SelbstRecht.Quellenrecht".equals(gsArt)) {
            return RealEstateType.DISTINCT_AND_PERMANENT_RIGHTS_RIGHT_TO_SPRING_WATER;
        } else if("SelbstRecht.Konzessionsrecht".equals(gsArt)) {
            return RealEstateType.DISTINCT_AND_PERMANENT_RIGHTS_CONCESSION;
        } else if("Bergwerk".equals(gsArt)) {
            return RealEstateType.MINERAL_RIGHTS;
        } else {
            throw new IllegalStateException("unknown gsArt");
        }        
    }
    
    private String getApplicationContext(UriInfo uriInfo) {
        return uriInfo.getRequestUri().toString().replace(uriInfo.getPath(), "");
    }
}
