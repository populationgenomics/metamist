import * as React from 'react'
import { Button } from 'semantic-ui-react'

interface PageOptionsProps {
    pageNumber: number
    handleOnClick: (e: number) => void
    total?: number
    totalPageNumbers: number
    isLoading: boolean
    title: string
}

const PageOptions: React.FunctionComponent<PageOptionsProps> = ({
    pageNumber,
    handleOnClick,
    total,
    totalPageNumbers,
    isLoading,
    title,
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
        {!!total && (
            <span style={{ padding: '8px 10px 0 10px' }}>
                Page {pageNumber} / {totalPageNumbers} ({total} {title})
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
