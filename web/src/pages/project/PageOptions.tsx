import * as React from 'react'
import { Button } from 'semantic-ui-react'

interface PageOptionsProps {
    pageNumber: number
    handleOnClick: (e: number) => void
    totalSamples?: number
    totalPageNumbers: number
    isLoading: boolean
}

const PageOptions: React.FunctionComponent<PageOptionsProps> = ({
    pageNumber,
    handleOnClick,
    totalSamples,
    totalPageNumbers,
    isLoading,
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
                Page {pageNumber} / {totalPageNumbers} ({totalSamples} samples)
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
