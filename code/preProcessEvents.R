# Pulling from Postgresql to R 
# data.table for processing

getMapEvents  <- function(mmsId, con){
  require(glue)
  require(DBI)
  
  (events  <- dbGetQuery(con,
               glue("select * FROM events WHERE map_mms_id = {mmsId}")))
}

parseEntities <- function(entity){
  require(jsonlite)

  rv <- parse_json(entity) 
  if (!("assister" %in% names(rv))){
    rv$assister <- NA
  }

  return(rv)
}

# Better function to get the firstPLayerDeaths for every round of a given
# mapMmsId
getFirstPlayerDeath <- function(mapMmsId, con){
  require(data.table)
  require(DBI)
  require(jsonlite)
  require(magrittr)
  require(glue)
  trimPhases <- function(phaseChanges){
    while (phaseChanges[1, name] != "round_start"){
      eventName <- phaseChanges[1, name]
      eventTick <- phaseChanges[1, tick]
      message(glue("Trimming {eventName}:{eventTick}"))
      phaseChanges <- phaseChanges[-1]
    }
    return(phaseChanges)
  }

  eventsBase <- as.data.table(
                              dbGetQuery(con, 
                               glue("SELECT *
                                FROM events
                                WHERE name IN 
                                  ('player_death', 'player_spawn', 'round_start',
                                   'round_freeze_end', 'bomb_planted', 
                                   'bomb_exploded', 'bomb_defused', 'round_end')
                                AND map_mms_id = {mapMmsId}
                                ORDER BY tick;")
                               )
  )

  message("Filter and Parse Event Death JSON")
  eventsDeath  <- eventsBase[name == "player_death"][,
                    # Get victim and attacker entity IDs for the tick for later
                    c("player", "assister", "attacker") := parseEntities(entities), by = id 
                                                     ] %>%
                    setnames("player", "victim.ent.index") %>%
                    setnames("attacker", "attacker.ent.index")

  message("Filter and Parse Player Spawn JSON")
  eventsPlayerSpawn <- eventsBase[name == 'player_spawn'][,
                         c("userid", "teamnum") := parse_json(data), by = id
                                                          ][, data := NULL]
  
  message("Filter and Parse Phase Changes")
  eventsPhaseChanges <- eventsBase[name %in% c("round_start",
                                               "round_freeze_end",
                                               "bomb_planted",
                                               "bomb_exploded",
                                               "bomb_defused",
                                               "round_end")] %>% trimPhases()
  message("Filter and Parse Round Starts")
  eventsRoundStart  <- eventsPhaseChanges[name == "round_start"][,
                        round_num := 1:.N][, 
                        .(tick, round_num, round_tick = tick)
                        ]
  setkey(eventsRoundStart, tick)
  setkey(eventsPhaseChanges, tick)

  eventsPhaseChanges <- eventsRoundStart[eventsPhaseChanges, roll=T]

  # Phase 1: Buy/freeze time in spawn
  eventsPhase1 <- eventsPhaseChanges[name == "round_start"][, 
                                                            .(tick, round_num)][,
                                                            phase := 1]

  # Phase 2: Normal play
  eventsPhase2 <- eventsPhaseChanges[name == "round_freeze_end"][,
                   .(tick, round_num)][, phase := 2][,
                   .(tick = min(tick)), by = .(round_num, phase) 
                   # There can be an extra round_freeze_end in the last round
                   # that's the same tick as the round_end that signals the end
                   # of the game
  ]

  # Phase 2.b: Post bomb plant
  eventsPhase2b <- eventsPhaseChanges[name == "bomb_planted"][,
                    .(tick, round_num)][, phase := "2.b"]

  # Phase 3: Post-round time (after T or CT win)
  eventsPhase3 <- eventsPhaseChanges[name == "round_end"][,
                    .(tick, round_num)][, phase := 3]

  # In the last round, there's no phase 3
  eventsPhase3 <- eventsPhase3[round_num != max(eventsPhaseChanges$round_num)]

  # Combine phases
  eventsPhasesAll <- rbind(eventsPhase1, eventsPhase2, eventsPhase2b,
                           eventsPhase3)
  setkey(eventsPhasesAll, round_num, tick)
  setkey(eventsPhaseChanges, round_num, tick)

  # Combine rounds and phases
  eventsRoundsPhases  <- eventsPhasesAll[eventsPhaseChanges, roll = -Inf][,
                           .(round_tick, phase_tick = tick, event_tick = tick,
                             round_num, phase)] %>% na.omit("phase")

  return(list(full = eventsRoundsPhases, test = eventsPhase2))
}





