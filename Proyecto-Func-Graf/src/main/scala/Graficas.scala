import cats.effect.IO
import com.github.tototoshi.csv.*
import org.nspl.*
import org.nspl.awtrenderer.*
import org.saddle.*
import cats.*
import doobie.*
import doobie.implicits.*
import java.io.File
import cats.effect.unsafe.implicits.global
import doobie.Transactor
import breeze.plot._

object Graficas {

  @main
  def integral2() =
    val path2DataFilePartidosYGoles: String = "C:\\Users\\Erick Gaona\\Desktop/\\proyecto\\dsPartidosYGoles.csv\\"
    val path2DataFile: String = "C:\\Users\\Erick Gaona\\Desktop/\\proyecto\\dsAlineacionesXTorneo.csv\\"
    val reader = CSVReader.open(new File(path2DataFile))
    val reader2 = CSVReader.open(new File(path2DataFilePartidosYGoles))

    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    val contentFile2:List[Map[String, String]] = reader2.allWithHeaders()
    reader.close()
    reader2.close()
    val rutaArchivo = "Graficas/BD_PartidosECantidad.png"


    //Conexion BD
    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/practicum",
      user = "root",
      password = "root123",
      logHandler = None
    )
    //Metodos graficos desde el CSV

    //densityNumeroC(contentFile)
    //frecuenciaGoles(contentFile2)
    //ganadoresdeMundiales(contentFile2)

    //Metodo graficos desde BD
    //CapacidadMaxE(EstadiosCapMgrafico().transact(xa).unsafeRunSync())
    graficaEdadGoles(edadGoles().transact(xa).unsafeRunSync())
    //graficaPromedioJug(promedioedadJug().transact(xa).unsafeRunSync())

    //CSV

    // numero de camiseta de los delanteros-> Genera grafico density plot
    def densityNumeroC(data: List[Map[String, String]]): Unit = {
      val listNroShirt: List[Double] = data
        .filter(row => row("squads_position_name") == "forward" && row("squads_shirt_number") != "0")
        .map(row => row("squads_shirt_number").toDouble)

      val densityforwardNumber = xyplot(density(listNroShirt.toVec.toSeq) -> line())(
        par
          .xlab("Numero de camiseta") // eje x
          .ylab("freq.") // eje y
          .main("Forward Shirt Number") // grafica nombre
      )

      pngToFile(new File("Graficas\\CSV_NumeroCamisetaDelanteros.png"), densityforwardNumber.build, 1000)

    }
    //Frecuencia de goles de cada mundial
    def frecuenciaGoles(data: List[Map[String,String]]): Unit = {
      val frecuenci = data
        .map(x => (x("matches_home_team_score"),x("matches_away_team_score")))
        .groupBy(_._2)
        .map(k=>(k._1,k._2.size.toDouble))


      val densitFrecuencia = xyplot(density(frecuenci.toSeq.map(_._2).toIndexedSeq) -> line())(


        par
          .xlab("Numero de Goles")
          .ylab("Frecuncia")
          .main("Frecuencia de goles Mundial")

      )
      pngToFile(new File("Graficas\\CSV_FrecuenciaGoles.png"), densitFrecuencia.build, 1000)

    }
    //Cuantos mundiales a ganado cada pais
    def ganadoresdeMundiales(data: List[Map[String, String]]) =
      val winners = data
        .map(row => (row("matches_tournament_id"), row("tournaments_winner")))
        .distinct
        .groupBy(_._2)
        .map(row => (row._1, row._2.size.toDouble))

      val ganadoresIndices = Index(winners.map(r => r._1).toArray)
      val valores = Vec(winners.map(valores => valores._2).toArray)

      val series = Series(ganadoresIndices, valores)
      val bar1 = saddle.barplotHorizontal(series,
        xLabFontSize = Option(RelFontSize(2)),
        color = RedBlue(80, 171))(
        par
          .xLabelRotation(-77)
          .xNumTicks(0)
          .xlab("Torneos")
          .ylab("Cantidad de mundiales")
          .main("Paises")
      )
      pngToFile(new File("Graficas\\CSV_FrecGanadores.png"), bar1.build, 1000)

  //BD
  // Mostrar los estadios y su capacidad en un país específico:
  def EstadiosCapMgrafico():ConnectionIO[List[(String, Double)]] = {
    sql"""
          SELECT s.name as StadiumName, s.capacity as Capacity
    FROM stadium s
    INNER JOIN country c ON s.countryId = c.countryId
    WHERE c.countryName = 'Argentina';
       """
      .query[(String, Double)]
      .to[List]
  }
  def CapacidadMaxE(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("Estadio")
        .ylab("Capacidad")
        .main("Capacidad Maxima")
    )

    pngToFile(new File("Graficas\\BD_EstadiosCap.png"),barPlot.build,5000)
  }

  // Gráfica de dispersión sobre la relación entre la edad de los jugadores (1990) y el número de goles que han marcado en los partidos.
  def edadGoles(): ConnectionIO[List[(Int, Int)]] = {
    sql"""
      SELECT (YEAR(CURRENT_DATE) - YEAR(birthDay)) AS edad, COUNT(goalId) AS total_goles
          FROM player
          INNER JOIN squad ON player.playerid = squad.playerid
          INNER JOIN goal ON squad.playerid = goal.playerId
          WHERE YEAR(birthDay) >=1990
          GROUP BY edad;
    """
      .query[(Int, Int)]
      .to[List]
  }
  def graficaEdadGoles(data: List[(Int, Int)]): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(data.map(_._1), data.map(_._2), '+' )
    p.xlabel = "Edad"
    p.ylabel = "Goles"
    f.saveas("Graficas\\BD_PartidosEGoles.png")

  }

  // Gráfica de promedio de edad de jugadores en funcion de su posicion del torneo de "2022"
  def promedioedadJug(): ConnectionIO[List[(String, Double)]] = {
    sql"""
      SELECT s.positionName, AVG(YEAR(CURDATE()) - YEAR(p.birthDay)) AS averageAge
    FROM squad s
    JOIN player p ON s.playerid = p.playerid
    JOIN tournament t ON s.tournamentid = t.tournamentid
    WHERE t.tournamentYear = 2022  -- Filtrar por el año del torneo
    GROUP BY s.positionName;
    """
      .query[(String, Double)]
      .to[List]
  }

  def graficaPromedioJug(data: List[(String, Double)]) = {
    val indices = Index(data.map(_._1).toArray)
    val values = Vec(data.map(_._2).toArray)

    val series = Series(indices, values)

    val barPlot = saddle.barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(0.5))
    )(
      par
        .xLabelRotation(-90)
        .xNumTicks(0)
        .xlab("Posiciones")
        .ylab("Promedio")
        .main("Promedio Edades- Posiciones")
    )

    pngToFile(
      new File("Graficas\\BD_PromedioJug.png"),
      barPlot.build,
      5000
    )
  }

      //grafica para obtener numero de partidos jugados en los estadios de cada capacidad
  def obtenerDatosEstadios(): ConnectionIO[List[(Int, Int)]] = {
    sql"""
        SELECT s.capacity, COUNT(*) AS num_partidos_jugados
            FROM matchs m
            JOIN stadium s ON m.stadiumId = s.stadiumId
            WHERE s.capacity > 60000
            GROUP BY s.capacity
      """.query[(Int, Int)].to[List]
  }

  //  generar el box plot
  def generarBoxPlot(data: List[(Int, Int)]): Unit = {
    val capacities = DenseVector(data.map(_._1.toDouble).toArray)
    val numPartidos = DenseVector(data.map(_._2.toDouble).toArray)

    val fig = Figure()
    val plt = fig.subplot(0)

    val boxPlot = plt += breeze.plot.plotting.BoxPlot(capacities, numPartidos, labels = List("Número de partidos jugados"))

    plt.xlabel = "Capacidad del estadio"
    plt.ylabel = "Número de partidos jugados"
    plt.title = "Capacidad del estadio y el número de partidos jugados"

    pngToFile(
      new File("Graficas\\BD_PartidosJuga.png"),
      barPlot.build, 5000)
  }

}

