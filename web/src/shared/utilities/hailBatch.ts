export function getHailBatchURL(batchId: string, jobId?: string | null): string {
    const base = `https://batch.hail.populationgenomics.org.au/batches/${batchId}`
    if (!jobId) {
        return base
    }
    return base + `/jobs/${jobId}`
}
