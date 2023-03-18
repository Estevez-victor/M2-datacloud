package datacloud.scala.tpfonctionnel.catalogue
import datacloud.scala.tpobject.catalogue.CatalogueWithNonMutable

class CatalogueSoldeWithFor extends CatalogueWithNonMutable with CatalogueSolde {
  def solde(x: Int): Unit = {
    for (i <- maMap) {
      storeProduct(i._1, i._2-(i._2*x/100) )
    }
  }
}