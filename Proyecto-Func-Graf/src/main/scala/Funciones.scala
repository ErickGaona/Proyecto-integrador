import com.github.tototoshi.csv.*

import java.io.File
import scala.io.Source
import org.nspl._
import org.nspl.awtrenderer._
import org.nspl.data.HistogramData

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object Funciones {
  @main
  def work() = {

    val path2DataFilePartidosYGoles: String = "C:\\Users\\Erick Gaona\\Desktop/\\proyecto\\dsPartidosYGoles.csv\\"
    val path2DataFileAlineacionesXTorneo: String = "C:\\Users\\Erick Gaona\\Desktop\\proyecto\\dsAlineacionesXTorneo.csv\\"

    val readerPartidosYGoles = CSVReader.open(new File(path2DataFilePartidosYGoles))
    val readerAlineacionesXTorneo = CSVReader.open(new File(path2DataFileAlineacionesXTorneo))

    val contentFilePartidosYGoles: List[Map[String, String]] = readerPartidosYGoles.allWithHeaders()
    val contentAlineacionesXTorneo: List[Map[String, String]] = readerAlineacionesXTorneo.allWithHeaders()

    readerPartidosYGoles.close()
    readerAlineacionesXTorneo.close()

    // 1.
    // Extraer la capacidad de los estadios y calcular la capacidad mínima, máxima y promedio
    val capacidad: List[Int] = contentFilePartidosYGoles
      .flatMap(_.get("stadiums_stadium_capacity")
      .filter(_.forall(_.isDigit)).map(_.toInt)) //filtra los caracteres de cada capacidad para quedarse solo con los dígitos
    
    val minCapacidad = capacidad.min
    val maxCapacidad = capacidad.max
    val promCapacidad = capacidad.sum / capacidad.length

    println(s"Capacidad mínima: $minCapacidad")
    println(s"Capacidad máxima: $maxCapacidad")
    println(s"Capacidad promedio: $promCapacidad")

    // 2.
    // ¿Cuál es el minuto más común en el que se han marcado un gol? 
    // Torneos masculinos y otra los torneos femeninos.
    //Hombres
    val minutoMasComunMasculino= contentFilePartidosYGoles
      .filter(x => x("tournaments_tournament_name").contains("Men")) //Filtra los que cumplan la clave y contienan la cadena "Men".
      .filter(_("goals_minute_regulation") != "NA") //Filtra los sean diferentes a "NA".
      .map(x => x("goals_minute_regulation")) // Mapeamos por la clave 
      .groupBy(identity) // Agrupamos los minutos de los goles para contar cuántas veces ocurre cada minuto. 
      .map(x => x._1 -> x._2.length) // Los mapeamos por clave(minutos) y valor(veces).
      .maxBy(_._2)._1 // Encuentramos el par clave-valor que tiene el valor máximo.

    // 3.
    //Mujeres
    val minutoMasComunFemenino = contentFilePartidosYGoles
      .filter(x => x("tournaments_tournament_name").contains("Women"))
      .filter(_("goals_minute_regulation") != "NA")
      .map(x => x("goals_minute_regulation"))
      .groupBy(identity)
      .map(x => x._1 -> x._2.length)
      .maxBy(_._2)._1

    println(s"Minuto más común en torneos masculinos: $minutoMasComunMasculino")
    println(s"Minuto más común en torneos femeninos: $minutoMasComunFemenino")

    // 4.
    // ¿Cuál es el periodo más común en los que se han marcado goles en todos los mundiales? 
    // Obtener todos los minutos de gol
    val minutos = contentFilePartidosYGoles
      .flatMap(_("goals_match_period").split(","))
    
    // Encontrar el periodo más común
    val periodo = minutos
      .groupBy(identity)
      .maxBy(_._2.length)._1

    // Resultado
    println(s"Periodo más común en el que se han marcado goles en todos los mundiales: $periodo")

    // 5.
    // ¿Cuál es el número de camiseta  más común que se utiliza en cada una de las posiciones
    // Agrupar por posición y número de camiseta y contar la frecuencia
    val posicionYnumero = contentAlineacionesXTorneo
    .groupBy(x => 
             (x("squads_position_name"), 
              x("squads_shirt_number")))
    .mapValues(_.size)

    // Encontrar el número de camiseta más común para cada posición
    val numerocamisetaComun = posicionYnumero
      .groupBy(_._1._1)
      .mapValues(_.maxBy(_._2)._1._2)

    numerocamisetaComun.foreach((position, shirtNumber) => println(s"Posición: $position, Número de camiseta más común: $shirtNumber"))

    // 6.
    // Encontrar la frecuencia de los marcadores en los mundiales de fútbol
    val frecuenciaMarcadores = contentFilePartidosYGoles
      .flatMap(x => 
               List(
                 x("matches_home_team_score"),
                 x("matches_away_team_score")))
      .groupBy(identity)
      .mapValues(_.size)

    // Resultado
    println("Frecuencia de los marcadores:")
    frecuenciaMarcadores.foreach { case (marcador, frecuencia) => println(s"$marcador: $frecuencia")
                                  
    }
  }
}
