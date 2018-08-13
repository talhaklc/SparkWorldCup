package com.udemy.spark.appl;

import java.io.Serializable;

/**
 * Created by tkilic on 20.07.2018.
 */
public class groupPlayer implements Serializable{
    String playerName;
    int matchCount;

    public groupPlayer(String playerName, int matchCount) {
        this.playerName = playerName;
        this.matchCount = matchCount;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public int getMatchCount() {
        return matchCount;
    }

    public void setMatchCount(int matchCount) {
        this.matchCount = matchCount;
    }
}
