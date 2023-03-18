package datacloud.scala.tpfonctionnel.catalogue
import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithNamedFunction extends CatalogueWithNonMutable with CatalogueSolde{
  def diminution(a:Double, percent:Int):Double = a * ((100.0 - percent) / 100.0)
  def solde(x:Int)={
      maMap=maMap.mapValues[Double]( diminution(_,x) )
  }
}
