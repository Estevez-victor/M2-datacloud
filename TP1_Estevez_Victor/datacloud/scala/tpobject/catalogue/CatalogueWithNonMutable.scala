package datacloud.scala.tpobject.catalogue

import scala.collection.mutable.ArrayBuffer

class CatalogueWithNonMutable extends Catalogue{
  var maMap :Map[String,Double]=Map()
  def getPrice(nom  : String)=
    if(maMap.contains(nom)){
      maMap(nom)
    }else{
      -1.0
    }
  def removeProduct(nom : String) = maMap=maMap-nom
  def selectProducts(min: Double, max:Double) :Iterable[String] = {
    var buff : ArrayBuffer[String]=ArrayBuffer()
    maMap.keys.foreach{ i => if( (maMap(i)>min) && (maMap(i)<max)){
      buff.append(i)  
    }
    }
    buff
  }
  def storeProduct(nom : String, prix: Double) : Unit = {
      if(maMap.contains(nom)){
        maMap=(maMap-nom)+((nom,prix))
      }else{
        maMap=maMap+((nom,prix))
      }
  }
}