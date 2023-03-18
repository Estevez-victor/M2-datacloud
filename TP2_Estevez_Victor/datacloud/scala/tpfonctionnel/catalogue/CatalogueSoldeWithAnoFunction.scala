package datacloud.scala.tpfonctionnel.catalogue
import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithAnoFunction extends CatalogueWithNonMutable with CatalogueSolde{
  def solde(x:Int)={
     maMap=maMap.mapValues( (a:Double) => a * ((100.0 - x) / 100.0) )
  }
}