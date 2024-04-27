/**
 * Returns a random integer in the inclusive integer range [lo, hi].
 *
 * @param {number} lo
 * @param {number} hi
 * @returns {number}
 */
export function randRange(lo: number, hi: number): number {
    return Math.floor(lo + Math.random() * (hi - lo + 1))
}

/**
 * Given a two-element array `bounds` of two numbers defining an inclusive
 * integer range, return a random element inside that range.
 *
 * @param {Array} bounds
 * @returns {number}
 */
export function randFromBounds(bounds: Array<number>): number { return randRange(bounds[0], bounds[1]) }

/**
 * Shuffles the given array in-place using the Fisher-Yates algorithm.
 *
 * @param {Array} arr
 */
export function shuffle(arr: Array<any>) {
    // Fisher-Yates shuffle
    for (let i = 0; i < arr.length-1; i++) {
        const j = Math.floor(i + Math.random() * (arr.length-i))
        const tmp = arr[i]
        arr[i] = arr[j]
        arr[j] = tmp
    }
}

/**
 * Gets a random set of numbers of size k in the range 0, 1, ... n-1. Returns
 * null if k > n.
 *
 * @param {number} n
 * @param {number} k
 * @returns {Array<number> | null}
 */
export function randomIndicesNoReplacement(n: number, k: number): Array<number> | null {
    if (n < k) { return null }
    let arr = Array.from({length: n}, (_, i) => i)
    for (let i = 0; i < k-1; i++) {
        const j = Math.floor(i + Math.random() * (arr.length-i))
        const tmp = arr[i]
        arr[i] = arr[j]
        arr[j] = tmp
    }
    return arr.slice(0, k)
}
