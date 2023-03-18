package datacloud.scala.tpobject.catalogue

import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

class CatalogueWithMutable extends Catalogue{
  var maMap :Map[String,Double]=Map()
  def getPrice(nom  : String)=
    if(maMap.contains(nom)){
      maMap(nom)
    }else{
      -1.0
    }
  def removeProduct(nom : String) = maMap.remove(nom)
  def selectProducts(min: Double, max:Double) :Iterable[String] = {
    var buff : ArrayBuffer[String]=ArrayBuffer()
    maMap.keys.foreach{ i => if( (maMap(i)>min) && (maMap(i)<max)){
      buff.append(i)  
    }
    }
    buff
  }
  def storeProduct(nom : String, prix: Double) : Unit = {
      maMap(nom)=prix
  }
}