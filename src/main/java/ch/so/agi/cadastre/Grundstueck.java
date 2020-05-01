package ch.so.agi.cadastre;

import org.locationtech.jts.geom.Geometry;

public class Grundstueck {
    private String egrid;
    private String nummer;
    private String nbident;
    private String art;
    private double flaechenmass;
    private Geometry geometrie; 
    private int bfsnr;
    private String gemeinde;
    private String kanton;
    private String gbSubKreis;
    private int gbSubKreisNummer;
    private String grundbuchamt;
    public String getEgrid() {
        return egrid;
    }
    public void setEgrid(String egrid) {
        this.egrid = egrid;
    }
    public String getNummer() {
        return nummer;
    }
    public void setNummer(String nummer) {
        this.nummer = nummer;
    }
    public String getNbident() {
        return nbident;
    }
    public void setNbident(String nbident) {
        this.nbident = nbident;
    }
    public String getArt() {
        return art;
    }
    public void setArt(String art) {
        this.art = art;
    }
    public double getFlaechenmass() {
        return flaechenmass;
    }
    public void setFlaechenmass(double flaechenmass) {
        this.flaechenmass = flaechenmass;
    }
    public Geometry getGeometrie() {
        return geometrie;
    }
    public void setGeometrie(Geometry geometrie) {
        this.geometrie = geometrie;
    }
    public int getBfsnr() {
        return bfsnr;
    }
    public void setBfsnr(int bfsnr) {
        this.bfsnr = bfsnr;
    }
    public String getGemeinde() {
        return gemeinde;
    }
    public void setGemeinde(String gemeinde) {
        this.gemeinde = gemeinde;
    }
    public String getKanton() {
        return kanton;
    }
    public void setKanton(String kanton) {
        this.kanton = kanton;
    }
    public String getGbSubKreis() {
        return gbSubKreis;
    }
    public void setGbSubKreis(String gbSubKreis) {
        this.gbSubKreis = gbSubKreis;
    }
    public int getGbSubKreisNummer() {
        return gbSubKreisNummer;
    }
    public void setGbSubKreisNummer(int gbSubKreisNummer) {
        this.gbSubKreisNummer = gbSubKreisNummer;
    }
    public String getGrundbuchamt() {
        return grundbuchamt;
    }
    public void setGrundbuchamt(String grundbuchamt) {
        this.grundbuchamt = grundbuchamt;
    }
}