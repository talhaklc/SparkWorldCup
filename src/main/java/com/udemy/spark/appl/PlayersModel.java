package com.udemy.spark.appl;

import java.io.Serializable;

/**
 * Created by tkilic on 20.07.2018.
 */
public class PlayersModel implements Serializable{

   // roundID,MatchID,Team,coachName,Line-up,shirtNumber,playerName,Position,Event

    String roundID;
    String matchID;
    String team;
    String coachTeam;
    String lineup;
    String shirtNumber;
    String playerName;
    String position;
    String event;

    public PlayersModel(String roundID, String matchID, String team, String coachTeam, String lineup, String shirtNumber, String playerName, String position, String event) {
        this.roundID = roundID;
        this.matchID = matchID;
        this.team = team;
        this.coachTeam = coachTeam;
        this.lineup = lineup;
        this.shirtNumber = shirtNumber;
        this.playerName = playerName;
        this.position = position;
        this.event = event;
    }

    public String getRoundID() {
        return roundID;
    }

    public void setRoundID(String roundID) {
        this.roundID = roundID;
    }

    public String getMatchID() {
        return matchID;
    }

    public void setMatchID(String matchID) {
        this.matchID = matchID;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getCoachTeam() {
        return coachTeam;
    }

    public void setCoachTeam(String coachTeam) {
        this.coachTeam = coachTeam;
    }

    public String getLineup() {
        return lineup;
    }

    public void setLineup(String lineup) {
        this.lineup = lineup;
    }

    public String getShirtNumber() {
        return shirtNumber;
    }

    public void setShirtNumber(String shirtNumber) {
        this.shirtNumber = shirtNumber;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }
}
