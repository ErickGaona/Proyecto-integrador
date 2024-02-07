// Importamos las librerias que estamos utilizando
import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import com.github.tototoshi.csv.*
import doobie.*
import doobie.implicits.*

import java.io.File
import java.io.{BufferedWriter, FileWriter}

//implicit object MyFormat extends DefaultCSVFormat{
//  override val delimiter = ';'
//}
object proyecto {
  @main
  def proyectoLeer() =
    // Direccion de acceso a los archivos
    val path = "C:\\Users\\steve\\Downloads\\practicum\\PartidosyGoles.csv"
    val path2 = "C:\\Users\\steve\\OneDrive\\Escritorio\\ProyectoFinal\\archivosCSV\\AlineacionesXTorneo.csv"

    // PartidosyGoles
    val reader = CSVReader.open(new File(path))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    // AlineacionesXTorneo
    val reader2 = CSVReader.open(new File(path2))
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()
    reader2.close()

    // Coneccion a la bse de datos
    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/prac1",
      user = "root",
      password = "root123",
      logHandler = None
    )

      // Primera Forma
//    generarDataCountry(contentFile)
//    generarDataTournament(contentFile)
//    generarDataStadium(contentFile)
//    generarDataHostCountry(contentFile)
//    generarDataTeam(contentFile)
//    generarDataPlayer(contentFile2)
//    generarDataSquad(contentFile2)
//    generarDataSquadSegundaForma(contentFile2)
//    generarDataMatch(contentFile)
//    generarDataGoal(contentFile)

      // Segunda Forma
//    generarDataCountrySegundaForma(contentFile).foreach(insert => insert.run.transact(xa).unsafeRunSync())
//    generarDataTournamentSegundaForma(contentFile).foreach(insert => insert.run.transact(xa).unsafeRunSync())
//    generarDataStadiumSegundaForma(contentFile).foreach(insert => insert.run.transact(xa).unsafeRunSync())




  def escribirDatosTXT(archivo: String): Unit =
    val rutaTXT = "C:\\Users\\steve\\Downloads\\practicum\\"
    val rutaFinal = rutaTXT + "Script"

    val escritor = new BufferedWriter(new FileWriter(rutaFinal, true))
    try {
      escritor.write(archivo)
      escritor.newLine()
    } finally {
      escritor.close()
    }

  def generarDataCountry(data: List[Map[String, String]]): Unit =
    val sqlinsert = s"INSERT INTO country(countryId, countryName) VALUES (%d, '%s');"
    val info = data
      .map(x => (
        x("id_away_team_name").toInt,
        x("away_team_name")
      ))
      .distinct
      .sorted
      .map(x => sqlinsert.format(x._1, x._2))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataCountrySegundaForma(data: List[Map[String, String]])=

    val info = data
      .map(x => (
        x("id_away_team_name").toInt,
        x("away_team_name")
      ))
      .distinct
      .sorted
      .map(x =>
        sql"""
            INSERT INTO country(countryId, countryName)
            VALUES(${x._1},${x._2});
         """.update)

    info

  def generarDataTournament(data: List[Map[String, String]]): Unit =

    val sqlinsert = s"INSERT INTO tournament(tournamentId, countryWinnerId, tournamentName, tournamentYear, " +
      s"tournamentCountTeams) VALUES ('%s', %d, '%s', %d, %d);"
    val info = data
      .map(x => (
        x("matches_tournament_id"),
        x("id_tournament_winner").toInt,
        x("tournaments_tournament_name").replaceAll("'", "\\\\'"),
        x("tournaments_year").toInt,
        x("tournaments_count_teams").toInt
      ))
      .distinct
      .sorted
      .map(x => sqlinsert.format(x._1, x._2, x._3, x._4, x._5))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataTournamentSegundaForma(data: List[Map[String, String]]) =

    val info = data
      .map(x => (
        x("matches_tournament_id"),
        x("id_tournament_winner").toInt,
        x("tournaments_tournament_name").replaceAll("'", "\\\\'"),
        x("tournaments_year").toInt,
        x("tournaments_count_teams").toInt
      ))
      .distinct
      .sorted
      .map(x =>
        sql"""
              INSERT INTO tournament(tournamentId, countryWinnerId, tournamentName, tournamentYear,tournamentCountTeams)
              VALUES(${x._1},${x._2}, ${x._3}, ${x._4}, ${x._5});
           """.update)
    info

  def generarDataStadium(data: List[Map[String, String]]): Unit =

    val sqlinsert = s"INSERT INTO stadium(stadiumId, countryid, name, cityName, " +
      s"capacity) VALUES ('%s', %d, '%s', '%s', %d);"
    val info = data
      .map(x => (
        x("matches_stadium_id"),
        x("id_stadiums_country_name").toInt,
        x("stadiums_stadium_name").replaceAll("'", "\\\\'"),
        x("stadiums_city_name").replaceAll("'", "\\\\'"),
        x("stadiums_stadium_capacity").toInt
      ))
      .distinct
      .sorted
      .map(x => sqlinsert.format(x._1, x._2, x._3, x._4, x._5))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataStadiumSegundaForma(data: List[Map[String, String]]) =

    val info = data
      .map(x => (
        x("matches_stadium_id"),
        x("id_stadiums_country_name").toInt,
        x("stadiums_stadium_name").replaceAll("'", "\\\\'"),
        x("stadiums_city_name").replaceAll("'", "\\\\'"),
        x("stadiums_stadium_capacity").toInt
      ))
      .distinct
      .sorted
      .map(x =>
        sql"""
            INSERT INTO stadium(stadiumId, countryid, name, cityName,capacity)
            VALUES(${x._1},${x._2}, ${x._3}, ${x._4}, ${x._5});
           """.update)
      info

  def generarDataHostCountry(data: List[Map[String, String]]): Unit =
    val sqlinsert = s"INSERT INTO hostCountry(countryId, tournamentId) VALUES (%d, '%s');"
    val info = data
      .filterNot(_("matches_tournament_id") == "WC-2002")
      .map(x => (
        x("id_tournaments_host_country").toInt,
        x("matches_tournament_id")
      ))
      .distinct
      .sorted
      .map(x => sqlinsert.format(x._1, x._2))

    val lista2002 = List(
      "INSERT INTO hostCountry(countryId, tournamentId) VALUES (44, 'WC-2002');",
      "INSERT INTO hostCountry(countryId, tournamentId) VALUES (71, 'WC-2002');"
    )
    val listasConcadenadas = lista2002 ++ info
    val infoString = listasConcadenadas.mkString("\n")
    println(info.length)
    //    info.foreach(println)
    escribirDatosTXT(infoString)

  def generarDataTeam(data: List[Map[String, String]]): Unit =
    val sqlInsert = s"INSERT INTO team(teamId, nameCountryId, mensTeam, womensTeam, regionName)" +
      s"VALUES('%s', %d, %d, %d, '%s');"
    val info = data
      .map(x =>
        (x("matches_away_team_id"),
          x("id_away_team_name").toInt,
          x("away_mens_team").toInt,
          x("away_mens_team").toInt,
          x("away_region_name")
        ))
      .distinct
      .sorted
      .map(x => sqlInsert.format(x._1, x._2, x._3, x._4, x._5))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataPlayer(data: List[Map[String, String]]): Unit =
    val sqlInsert = s"INSERT INTO player(playerId, familyName, givenName, birthDay, female, goalKeeper, defender, " +
      s"midfielder, forward) VALUES ('%s','%s','%s', %s, %d, %d, %d, %d, %d);"
    val info = data
      .map(x =>
        (x("squads_player_id"),
          x("players_family_name").replaceAll("'", "\\\\'"),
          x("players_family_name").replaceAll("'", "\\\\'"),
          if (x("players_birth_date") == "not available" || x("players_birth_date") == "") null else s"'${x("players_birth_date")}'",
          x("players_female").toInt,
          x("players_goal_keeper").toInt,
          x("players_defender").toInt,
          x("players_midfielder").toInt,
          x("players_forward").toInt
        ))
      .distinct
      .sorted
      .map(x => sqlInsert.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataSquad(data: List[Map[String, String]]): Unit =
    val sqlFormat = s"INSERT INTO squad(playerid, tournamentid, teamId, shirtNumber, positionName) " +
      s"VALUES('%s','%s', '%s', %d, '%s');"

    val info = data
      .map(x => (
        x("squads_player_id"),
        x("squads_tournament_id"),
        x("squads_team_id"),
        x("squads_shirt_number").toInt,
        x("squads_position_name")
      ))
      .sorted
      .map(x => sqlFormat.format(x._1, x._2, x._3, x._4, x._5))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataSquadSegundaForma(data: List[Map[String, String]]): Unit =
//    val sqlFormat = s"INSERT INTO squad(playerid, tournamentid, teamId, shirtNumber, positionName) " +
//      s"VALUES('%s','%s', '%s', %d, '%s');"

    val info = data
      .map(x => (
        x("squads_player_id"),
        x("squads_tournament_id"),
        x("squads_team_id"),
        x("squads_shirt_number").toInt,
        x("squads_position_name")
      ))
      .sorted
      .map(x =>
        sql"""
              INSERT INTO squad(playerid, tournamentid, teamId, shirtNumber, positionName)
              VALUES(${x._1},${x._2}, ${x._3}, ${x._4}, ${x._5});
           """.update)

    info
//    println(info.length)

  def generarDataMatch(data: List[Map[String, String]]): Unit =

    val sqlFormat = s"INSERT INTO matchs(matchId, stadiumId, tournamentId, homeTeamId, awayTeamId, " +
      s"matchDate, matchTime, stageName, homeTeamScore, awayTeamScore, extraTime, penaltyShootout, " +
      s"homeTeamScorePenalties, awayTeamScorePenalties, result) " +
      s"VALUES('%s','%s', '%s','%s', '%s', '%s', '%s', '%s', %d, %d, %f, %d, %d, %d, '%s');"
    val info = data
      .map(x => (
        x("matches_match_id"),
        x("matches_stadium_id"),
        x("matches_tournament_id"),
        x("matches_home_team_id"),
        x("matches_away_team_id"),
        x("matches_match_date"),
        x("matches_match_time"),
        x("matches_stage_name"),
        x("matches_home_team_score").toInt,
        x("matches_away_team_score").toInt,
        x("matches_extra_time").toDouble,
        x("matches_penalty_shootout").toInt,
        x("matches_home_team_score_penalties").toInt,
        x("matches_away_team_score_penalties").toInt,
        x("matches_result"))
      )
      .distinct
      .map(x => sqlFormat.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)

  def generarDataGoal(data: List[Map[String, String]]): Unit =

    val sqlFormat = s"INSERT INTO goal(goalId, matchId, playerId, minuteLabel, minuteRegulation, " +
      s"minuteStoppage, matchPeriod, ownGoal, penalty)" +
      s"VALUES('%s','%s', '%s', '%s', %d, %d, '%s', %d, %d);"

    val info = data
      .filterNot(_("goals_goal_id") == "NA")
      .map(x => (
        x("goals_goal_id"),
        x("matches_match_id"),
        x("goals_player_id"),
        x("goals_minute_label").replaceAll("'", "\\\\'"),
        x("goals_minute_regulation").toInt,
        x("goals_minute_stoppage").toInt,
        x("goals_match_period"),
        x("goals_own_goal").toInt,
        x("goals_penalty").toInt
      ))
      .distinct
      .sorted
      .map(x => sqlFormat.format(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9))

    println(info.length)
    //    info.foreach(println)
    val infoString = info.mkString("\n")
    escribirDatosTXT(infoString)



}
