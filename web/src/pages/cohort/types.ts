export interface SequencingGroup {
  id: string
  type: string
  technology: string
  platform: string
  project: { id: number; name: string }
}
