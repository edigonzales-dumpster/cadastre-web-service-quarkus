<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format" xmlns:fox="http://xmlgraphics.apache.org/fop/extensions" xmlns:extract="http://geo.so.ch/schema/AGI/Cadastre/0.9/Extract" exclude-result-prefixes="extract" version="1.0">
  <xsl:output method="xml" indent="yes"/>
  <xsl:decimal-format name="swiss" decimal-separator="." grouping-separator="'"/>
  <xsl:template match="extract:GetExtractByIdResponse">
    <fo:root xmlns:fo="http://www.w3.org/1999/XSL/Format" xmlns:xsd="https://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" font-family="Cadastra" language="de">
      <fo:layout-master-set>
        <fo:simple-page-master master-name="firstPage" page-height="297mm" page-width="210mm" margin-top="10mm" margin-bottom="10mm" margin-left="12mm" margin-right="10mm">
          <fo:region-body margin-top="35mm" background-color="#FFFFFF"/>
          <fo:region-before extent="30mm" background-color="#FFFFFF"/>
          <fo:region-after extent="10mm" background-color="#FFFFFF" display-align="after"/>
        </fo:simple-page-master>
        <fo:simple-page-master master-name="middlePage" page-height="297mm" page-width="210mm" margin-top="10mm" margin-bottom="10mm" margin-left="12mm" margin-right="10mm">
          <fo:region-body margin-top="15mm" background-color="#FFFFFF"/>
          <fo:region-before extent="10mm" background-color="#FFFFFF"/>
          <fo:region-after extent="10mm" background-color="#FFFFFF" display-align="after"/>
        </fo:simple-page-master>
        <fo:page-sequence-master master-name="allPages">
          <fo:repeatable-page-master-alternatives>
            <fo:conditional-page-master-reference page-position="first" master-reference="firstPage"/>
            <fo:conditional-page-master-reference page-position="rest" master-reference="middlePage"/>
          </fo:repeatable-page-master-alternatives>
        </fo:page-sequence-master>
      </fo:layout-master-set>
      <xsl:apply-templates/>
    </fo:root>
  </xsl:template>
  <xsl:template match="extract:Extract">
    <fo:page-sequence master-reference="allPages" id="my-sequence-id">
      <fo:static-content flow-name="xsl-region-before">
        <fo:block>
          <fo:block-container margin="0mm" padding="0mm" space-before="0mm" absolute-position="absolute" top="0mm" left="0mm" height="6.7mm" background-color="#FFFFFF">
            <fo:block margin="0mm" padding="0mm" space-before="0mm" line-height="6.7mm" line-stacking-strategy="font-height" font-size="13mm">
              <fo:external-graphic height="6.7mm" width="60mm" content-width="scale-to-fit" content-height="scale-to-fit" fox:alt-text="CantonalLogo">
                <xsl:attribute name="src">
                  <xsl:text>url('data:</xsl:text>
                  <xsl:text>image/png;base64,</xsl:text>
                  <xsl:value-of select="extract:CantonalLogo"/>
                  <xsl:text>')</xsl:text>
                </xsl:attribute>
              </fo:external-graphic>
            </fo:block>
          </fo:block-container>
          <fo:block-container margin="0mm" padding="0mm" space-before="0mm" absolute-position="absolute" top="0mm" left="95mm" height="13mm" background-color="#FFFFFF">
            <fo:block margin="0mm" padding="0mm" space-before="0mm" line-height="13mm" line-stacking-strategy="font-height" font-size="13mm">
              <fo:external-graphic margin="0mm" padding="0mm" space-before="0mm" vertical-align="top" width="30mm" height="13mm" scaling="uniform" content-width="scale-to-fit" content-height="scale-to-fit" fox:alt-text="MunicipalityLogo">
                <xsl:attribute name="src">
                  <xsl:text>url('data:</xsl:text>
                  <xsl:text>image/png;base64,</xsl:text>
                  <xsl:value-of select="extract:MunicipalityLogo"/>
                  <xsl:text>')</xsl:text>
                </xsl:attribute>
              </fo:external-graphic>
            </fo:block>
          </fo:block-container>
          <!--https://stackoverflow.com/questions/46321155/external-graphic-has-space-above-even-though-ive-set-it-to-zero-->
          <fo:block-container margin="0mm" padding="0mm" space-before="0mm" absolute-position="absolute" top="0mm" left="139mm" height="7.5mm">
            <fo:block margin="0mm" padding="0mm" space-before="0mm" line-height="7.5mm" line-stacking-strategy="font-height" font-size="7.5mm">
              <fo:external-graphic margin="0mm" padding="0mm" space-before="0mm" vertical-align="top" width="49mm" height="7.5mm" scaling="non-uniform" content-width="scale-to-fit" content-height="scale-to-fit" fox:alt-text="GrundstuecksinformationLogo">
                <xsl:attribute name="src">
                  <xsl:text>url('data:</xsl:text>
                  <xsl:text>image/png;base64,</xsl:text>
                  <xsl:value-of select="extract:LogoGrundstuecksinformation"/>
                  <xsl:text>')</xsl:text>
                </xsl:attribute>
              </fo:external-graphic>
            </fo:block>
          </fo:block-container>
        </fo:block>
        <fo:block>
          <fo:block-container absolute-position="absolute" top="19mm" left="0mm">
            <fo:block font-size="0pt" padding="0mm" margin="0mm" line-height="0mm">
              <fo:leader leader-pattern="rule" leader-length="100%" rule-style="solid" rule-thickness="0.4pt"/>
            </fo:block>
          </fo:block-container>
        </fo:block>
        <fo:retrieve-marker retrieve-class-name="subHeader" retrieve-position="first-starting-within-page"/>
      </fo:static-content>
      <fo:static-content flow-name="xsl-region-after">
        <fo:block font-size="0pt" padding="0mm" margin="0mm" line-height="0mm">
          <fo:leader leader-pattern="rule" leader-length="100%" rule-style="solid" rule-thickness="0.8pt"/>
        </fo:block>
        <fo:table table-layout="fixed" width="100%" margin-top="1mm" font-size="6.5pt" font-style="normal" font-weight="400" font-family="Cadastra">
          <fo:table-column column-width="50%"/>
          <fo:table-column column-width="50%"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell>
                <fo:block>
                  <xsl:value-of select="format-dateTime(extract:CreationDate,'[Y0001]-[M01]-[D01] [H01]:[m01]:[s01]')"/>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right">
                <fo:block>Seite <fo:page-number/>/<fo:page-number-citation-last ref-id="my-sequence-id"/></fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
      </fo:static-content>
      <xsl:apply-templates select="extract:RealEstate"/>
    </fo:page-sequence>
  </xsl:template>
  <xsl:template match="extract:RealEstate">
    <fo:flow flow-name="xsl-region-body">
      <fo:block-container wrap-option="wrap" hyphenate="false" hyphenation-character="-" font-weight="700" font-size="14pt">
        <fo:table table-layout="fixed" width="100%">
          <fo:table-column column-width="94mm"/>
          <fo:table-column column-width="94mm"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell>
                <fo:block>Grundstücksbeschrieb</fo:block>
              </fo:table-cell>
              <fo:table-cell>
                <fo:block>GB-Nr. <xsl:value-of select="extract:Number"/></fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
      </fo:block-container>
      <fo:block-container wrap-option="wrap" hyphenate="false" hyphenation-character="-" font-weight="400" font-size="8.5pt">
        <fo:table table-layout="fixed" width="100%" margin-top="8mm">
          <fo:table-column column-width="50mm"/>
          <fo:table-column column-width="25mm"/>
          <fo:table-column column-width="19mm"/>
          <fo:table-column column-width="50mm"/>
          <fo:table-column column-width="25mm"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Gemeinde:</fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block>
                  <xsl:value-of select="extract:Municipality"/>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block/>
              </fo:table-cell>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>E-GRID:</fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block>
                  <xsl:value-of select="extract:EGRID"/>
                </fo:block>
              </fo:table-cell>
            </fo:table-row>
            <fo:table-row>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Grundbuch:</fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block>
                  <xsl:value-of select="extract:SubunitOfLandRegister"/>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block/>
              </fo:table-cell>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>NBIdent:</fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block>
                  <xsl:value-of select="extract:IdentND"/>
                </fo:block>
              </fo:table-cell>
            </fo:table-row>
            <fo:table-row>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Grundstücksart:</fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block>
                  <xsl:choose>
                    <xsl:when test="extract:Type = 'RealEstate'">
                      <xsl:text>Liegenschaft</xsl:text>
                    </xsl:when>
                    <xsl:when test="extract:Type = 'Distinct_and_permanent_rights.BuildingRight'">
                      <xsl:text>Baurecht</xsl:text>
                    </xsl:when>
                    <xsl:when test="extract:Type = 'Distinct_and_permanent_rights.right_to_spring_water'">
                      <xsl:text>Quellenrecht</xsl:text>
                    </xsl:when>
                    <xsl:when test="extract:Type = 'Distinct_and_permanent_rights.concession'">
                      <xsl:text>Konzessionsrecht</xsl:text>
                    </xsl:when>
                    <xsl:when test="extract:Type = 'Distinct_and_permanent_rights.other'">
                      <xsl:text>weiteres selbständiges und dauerendes Recht</xsl:text>
                    </xsl:when>
                    <xsl:when test="extract:Type = 'Mineral_rights'">
                      <xsl:text>Bergwerk</xsl:text>
                    </xsl:when>
                    <xsl:otherwise>
                      <xsl:text>should not reach here</xsl:text>
                    </xsl:otherwise>
                  </xsl:choose>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block/>
              </fo:table-cell>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Grundstücksfläche:</fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="right" padding-top="2mm">
                <fo:block line-height-shift-adjustment="disregard-shifts"><xsl:value-of select="format-number(extract:LandRegistryArea, &quot;#'###&quot;, &quot;swiss&quot;)"/> m<fo:inline baseline-shift="super" font-size="60%">2</fo:inline></fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
      </fo:block-container>
      <fo:block-container wrap-option="wrap" hyphenate="false" hyphenation-character="-" font-weight="400" font-size="8.5pt" background-color="#FFFFFF">
        <fo:table table-layout="fixed" width="100%" margin-top="8mm">
          <fo:table-column column-width="188mm"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Gebäude:</fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
        <fo:table table-layout="fixed" width="100%" margin-top="1mm" font-size="6.5pt">
          <fo:table-column column-width="28mm"/>
          <fo:table-column column-width="14mm"/>
          <fo:table-column column-width="12mm"/>
          <fo:table-column column-width="20mm"/>
          <fo:table-column column-width="20mm"/>
          <fo:table-column column-width="94mm"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell padding-top="2mm">
                <fo:block>EGID</fo:block>
              </fo:table-cell>
              <fo:table-cell padding-top="2mm">
                <fo:block text-align="right">Fläche</fo:block>
              </fo:table-cell>
              <fo:table-cell padding-top="2mm">
                <fo:block/>
              </fo:table-cell>
              <fo:table-cell text-align="left" padding-top="2mm">
                <fo:block>
                  <fo:block>projektiert</fo:block>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell text-align="left" padding-top="2mm">
                <fo:block>unterirdisch</fo:block>
              </fo:table-cell>
              <fo:table-cell padding-top="2mm">
                <fo:block>Adressen</fo:block>
              </fo:table-cell>
            </fo:table-row>
            <xsl:for-each select="extract:Building">
              <xsl:sort select="extract:Egid"/>
              <fo:table-row border-width="0pt" border-style="solid" font-size="8.5pt">
                <fo:table-cell padding-top="1mm" border-width="0pt" border-style="solid">
                  <fo:block>
                    <xsl:choose>
                      <xsl:when test="extract:Egid">
                        <xsl:value-of select="extract:Egid"/>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:text>–</xsl:text>
                      </xsl:otherwise>
                    </xsl:choose>
                  </fo:block>
                </fo:table-cell>
                <fo:table-cell padding-top="1mm" border-width="0pt" border-style="solid">
                  <fo:block text-align="right" margin-right="1mm">
                    <xsl:value-of select="format-number(extract:Area, &quot;#'###&quot;, &quot;swiss&quot;)"/>
                  </fo:block>
                </fo:table-cell>
                <fo:table-cell padding-top="1mm">
                  <fo:block margin-left="0mm" line-height-shift-adjustment="disregard-shifts">m<fo:inline baseline-shift="super" font-size="60%">2</fo:inline></fo:block>
                </fo:table-cell>
                <fo:table-cell padding-top="1mm" border-width="0pt" border-style="solid">
                  <fo:block>
                    <xsl:choose>
                      <xsl:when test="extract:planned = 'true'">
                        <xsl:text>ja</xsl:text>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:text>nein</xsl:text>
                      </xsl:otherwise>
                    </xsl:choose>
                  </fo:block>
                </fo:table-cell>
                <fo:table-cell padding-top="1mm" border-width="0pt" border-style="solid">
                  <fo:block>
                    <xsl:choose>
                      <xsl:when test="extract:undergroundStructure = 'true'">
                        <xsl:text>ja</xsl:text>
                      </xsl:when>
                      <xsl:otherwise>
                        <xsl:text>nein</xsl:text>
                      </xsl:otherwise>
                    </xsl:choose>
                  </fo:block>
                </fo:table-cell>
                <fo:table-cell padding-top="1mm" border-width="0pt" border-style="solid">
                  <!-- Es wird immer mindestens ein leerer Block geschrieben. Oder choose/when/otherwise. -->
                  <fo:block/>
                  <xsl:for-each select="extract:BuildingEntry">
                    <fo:block>
                      <xsl:value-of select="extract:PostalAddress/extract:Street"/>
                      <xsl:text> </xsl:text>
                      <xsl:value-of select="extract:PostalAddress/extract:Number"/>
                      <xsl:text>, </xsl:text>
                      <xsl:value-of select="extract:PostalAddress/extract:PostalCode"/>
                      <xsl:text> </xsl:text>
                      <xsl:value-of select="extract:PostalAddress/extract:City"/>
                    </fo:block>
                  </xsl:for-each>
                </fo:table-cell>
              </fo:table-row>
            </xsl:for-each>
          </fo:table-body>
        </fo:table>
      </fo:block-container>
      <fo:block-container language="de" wrap-option="wrap" hyphenate="true" hyphenation-character="-" font-weight="400" font-size="8.5pt">
        <fo:table table-layout="fixed" width="100%" margin-top="8mm">
          <fo:table-column column-width="94mm"/>
          <fo:table-column column-width="94mm"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Bodenbedeckung:</fo:block>
              </fo:table-cell>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Flurnamen:</fo:block>
              </fo:table-cell>
            </fo:table-row>
            <fo:table-row>
              <fo:table-cell font-weight="400" padding-top="1mm">
                <fo:block>
                  <fo:table table-layout="fixed" width="100%" margin-top="0mm">
                    <fo:table-column column-width="50mm"/>
                    <fo:table-column column-width="20mm"/>
                    <fo:table-column column-width="5mm"/>
                    <fo:table-body border-width="0pt" border-style="solid">
                      <xsl:for-each select="extract:LandCoverShare">
                        <xsl:sort select="upper-case(extract:TypeDescription)"/>
                        <fo:table-row border-width="0pt" border-style="solid">
                          <fo:table-cell padding-top="1mm" border-style="solid" border-width="0pt">
                            <fo:block>
                              <xsl:value-of select="extract:TypeDescription"/>
                            </fo:block>
                          </fo:table-cell>
                          <fo:table-cell padding-top="1mm" border-style="solid" border-width="0pt">
                            <fo:block text-align="right">
                              <xsl:value-of select="format-number(extract:Area, &quot;#'###&quot;, &quot;swiss&quot;)"/>
                            </fo:block>
                          </fo:table-cell>
                          <fo:table-cell padding-top="1mm" border-style="solid" border-width="0pt">
                            <fo:block text-align="right" margin-left="1mm" line-height-shift-adjustment="disregard-shifts">m<fo:inline baseline-shift="super" font-size="60%">2</fo:inline></fo:block>
                          </fo:table-cell>
                        </fo:table-row>
                      </xsl:for-each>
                      <fo:table-row border-width="0pt" border-style="solid" font-weight="400" font-style="italic">
                        <fo:table-cell padding-top="1mm">
                          <fo:block>Total</fo:block>
                        </fo:table-cell>
                        <fo:table-cell padding-top="1mm">
                          <fo:block text-align="right">
                            <xsl:value-of select="format-number(sum(extract:LandCoverShare/extract:Area), &quot;#'###&quot;, &quot;swiss&quot;)"/>
                          </fo:block>
                        </fo:table-cell>
                        <fo:table-cell padding-top="1mm">
                          <fo:block text-align="right" margin-left="1mm" line-height-shift-adjustment="disregard-shifts">m<fo:inline baseline-shift="super" font-size="60%">2</fo:inline></fo:block>
                        </fo:table-cell>
                      </fo:table-row>
                    </fo:table-body>
                  </fo:table>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell font-weight="400" padding-top="2mm">
                <fo:block>
                  <xsl:for-each select="extract:LocalName">
                    <xsl:sort select="extract:Name" order="ascending"/>
                    <xsl:value-of select="extract:Name"/>
                    <xsl:if test="position() != last()">, </xsl:if>
                  </xsl:for-each>
                </fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
      </fo:block-container>
      <fo:block-container wrap-option="wrap" hyphenate="false" hyphenation-character="-" font-weight="400" font-size="8.5pt">
        <fo:table table-layout="fixed" width="100%" margin-top="8mm">
          <fo:table-column column-width="94mm"/>
          <fo:table-column column-width="94mm"/>
          <fo:table-body>
            <fo:table-row>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Grundbuchamt:</fo:block>
              </fo:table-cell>
              <fo:table-cell font-weight="700" padding-top="2mm">
                <fo:block>Nachführungsgeometer:</fo:block>
              </fo:table-cell>
            </fo:table-row>
            <fo:table-row>
              <!--preserve anders lösen? Mit blocks?-->
              <fo:table-cell font-weight="400" padding-top="2mm">
                <fo:block linefeed-treatment="preserve">
                  <xsl:value-of select="extract:LandRegisterOffice/extract:Name"/>
                  <xsl:text>
                  </xsl:text>
                  <xsl:if test="extract:LandRegisterOffice/extract:Line1">
                    <xsl:value-of select="extract:LandRegisterOffice/extract:Line1"/>
                    <xsl:text> 
                  </xsl:text>
                  </xsl:if>
                  <xsl:value-of select="extract:LandRegisterOffice/extract:Address/extract:Street"/>
                  <xsl:text> </xsl:text>
                  <xsl:value-of select="extract:LandRegisterOffice/extract:Address/extract:Number"/>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:value-of select="extract:LandRegisterOffice/extract:Address/extract:PostalCode"/>
                  <xsl:text> </xsl:text>
                  <xsl:value-of select="extract:LandRegisterOffice/extract:Address/extract:City"/>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:text>Telefon </xsl:text>
                  <xsl:value-of select="extract:LandRegisterOffice/extract:Phone"/>
                  <xsl:text> 
                  </xsl:text>
                  <fo:basic-link text-decoration="none" color="rgb(76,143,186)">
                    <xsl:attribute name="external-destination">mailto:<xsl:value-of select="extract:LandRegisterOffice/extract:Email"/></xsl:attribute>
                    <xsl:value-of select="extract:LandRegisterOffice/extract:Email"/>
                  </fo:basic-link>
                  <xsl:text> 
                  </xsl:text>
                  <fo:basic-link text-decoration="none" color="rgb(76,143,186)"><xsl:attribute name="external-destination"><xsl:value-of select="extract:LandRegisterOffice/extract:Web"/></xsl:attribute>so.ch
                  </fo:basic-link>
                </fo:block>
              </fo:table-cell>
              <fo:table-cell font-weight="400" padding-top="2mm">
                <fo:block linefeed-treatment="preserve">
                  <xsl:value-of select="extract:SurveyorOffice/extract:Person/extract:FirstName"/>
                  <xsl:text> </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Person/extract:LastName"/>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Name"/>
                  <xsl:value-of select="extract:SurveyorOffice/extract:PostalAddress/extract:Line1"/>
                  <xsl:if test="extract:SurveyorOffice/extract:Line1">
                    <xsl:text> 
                    </xsl:text>
                    <xsl:value-of select="extract:SurveyorOffice/extract:Line1"/>
                  </xsl:if>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Address/extract:Street"/>
                  <xsl:text> </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Address/extract:Number"/>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Address/extract:PostalCode"/>
                  <xsl:text> </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Address/extract:City"/>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:text> 
                  </xsl:text>
                  <xsl:text>Telefon </xsl:text>
                  <xsl:value-of select="extract:SurveyorOffice/extract:Phone"/>
                  <xsl:text> 
                  </xsl:text>
                  <fo:basic-link text-decoration="none" color="rgb(76,143,186)">
                    <xsl:attribute name="external-destination">mailto:<xsl:value-of select="extract:SurveyorOffice/extract:Email"/></xsl:attribute>
                    <xsl:value-of select="extract:SurveyorOffice/extract:Email"/>
                  </fo:basic-link>
                  <xsl:text> 
                  </xsl:text>
                  <fo:basic-link text-decoration="none" color="rgb(76,143,186)">
                    <xsl:attribute name="external-destination">https://<xsl:value-of select="extract:SurveyorOffice/extract:Web"/></xsl:attribute>
                    <xsl:value-of select="extract:SurveyorOffice/extract:Web"/>
                  </fo:basic-link>
                </fo:block>
              </fo:table-cell>
            </fo:table-row>
          </fo:table-body>
        </fo:table>
      </fo:block-container>
    </fo:flow>
  </xsl:template>
</xsl:stylesheet>
