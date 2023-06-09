import * as React from 'react'
import { Button } from 'semantic-ui-react'

interface PageOptionsProps {
    pageNumber: number
    handleOnClick: (e: number) => void
    totalSamples?: number
    totalPageNumbers: number
    isLoading: boolean
    title?: string
}

const PageOptions: React.FunctionComponent<PageOptionsProps> = ({
    pageNumber,
    handleOnClick,
    totalSamples,
    totalPageNumbers,
    isLoading,
    title = 'samples',
}) => (
    <>
        {pageNumber > 1 && (
            <Button
                style={{ marginLeft: '10px' }}
                disabled={isLoading}
                onClick={() => {
                    handleOnClick(pageNumber - 1)
                }}
            >
                Previous
            </Button>
        )}
        {!!totalSamples && (
            <span style={{ padding: '8px 10px 0 10px' }}>
                Page {pageNumber} / {totalPageNumbers} ({totalSamples} {title})
            </span>
        )}
        {pageNumber < totalPageNumbers && (
            <Button
                disabled={isLoading}
                onClick={() => {
                    handleOnClick(pageNumber + 1)
                }}
            >
                Next
            </Button>
        )}
    </>
)

export default PageOptions
