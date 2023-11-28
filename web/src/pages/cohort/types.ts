export interface SequencingGroup {
  id: string
  type: string
  technology: string
  platform: string
  project: { id: number; name: string }
}

export interface Project {
  id: number
  name: string
}