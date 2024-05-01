import { RACE_LENGTH, SERVER_TICK_RATE_MS } from "../config/globalsettings.js"
import { InternalHorse } from "./horse/horse.js"
import { Race } from "./race.js"

export class RaceState {
    horseStates: Array<HorseState> = []
    rankings: Array<number> = []
    time: number = 0
    length: number = RACE_LENGTH

    constructor(race?: Race) {
        if (race) {
            this.horseStates = race.horses.map((h) => new HorseState(h))
            this.rankings = Array.from(
                {length: this.horseStates.length},
                (_, i) => i)
        }
    }

    nextState(): RaceState {
        let next = new RaceState()        
        next.time = this.time + SERVER_TICK_RATE_MS
        next.horseStates = this.horseStates.map((hs) => {
            if (hs.finishTime !== null) { return hs }
            let nextHs = new HorseState(hs.horse)
            nextHs.currentSpeed = 
                Math.min(hs.currentSpeed + hs.horse.acceleration,
                         hs.horse.topSpeed)
            nextHs.position = hs.position + hs.currentSpeed
            if (nextHs.position >= RACE_LENGTH) {
                nextHs.position = RACE_LENGTH
                nextHs.finishTime = next.time
            }

            return nextHs
        })
        next.rankings = Array.from(
            {length: this.horseStates.length},
            (_, i) => i)
        next.recomputeRankings()
         
        return next
    }

    recomputeRankings() {
        this.rankings.sort((idxI, idxJ) => {
            const horseI = this.horseStates[idxI]
            const horseJ = this.horseStates[idxJ]
            if (horseI.finishTime !== null && horseJ.finishTime !== null) {
                if (Math.abs(horseI.finishTime - horseJ.finishTime) > 0) {
                    return horseI.finishTime - horseJ.finishTime
                } else {
                    return idxI - idxJ
                }
            } else if (horseI.finishTime !== null) {
                return -1
            } else if (horseJ.finishTime !== null) {
                return 1
            } else {
                if (Math.abs(horseI.position - horseJ.position) > 0) {
                    return horseJ.position - horseI.position
                } else {
                    return idxI - idxJ
                }
            }
        })
    }

    raceOver() {
        return this.horseStates.every((hs) => hs.finishTime !== null)
    }
}

export class HorseState {
    horse: InternalHorse 

    position: number = 0
    currentSpeed: number = 0
    finishTime: number | null = null

    constructor(horse: InternalHorse) {
        this.horse = horse
    }
}
