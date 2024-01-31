export interface SequencingGroup {
  id: string
  type: string
  technology: string
  platform: string
  project: { id: number; name: string }
  assayMeta: object[]
}

export interface Project {
  id: number
  name: string
}

export interface APIError {
    name: string
    description: string
    stacktrace: string
}
