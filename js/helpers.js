export function isodate(date) {
  return date.toISOString().substring(0, 10)
}

export default {isodate}
