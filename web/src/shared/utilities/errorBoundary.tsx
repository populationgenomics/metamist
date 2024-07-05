import React, { Component, ReactNode } from 'react'

import { Button, Message } from 'semantic-ui-react'

interface Props {
    title?: string | ReactNode
    showInAlert?: boolean
    children?: ReactNode
}

interface State {
    error?: Error
    showDetails: boolean
}

class ErrorBoundary extends Component<Props, State> {
    public state: State = { showDetails: false }

    public static getDerivedStateFromError(error: Error): { error: Error } {
        // Update state so the next render will show the fallback UI.
        return { error }
    }

    public render(): React.ReactNode {
        if (!this.state.error) {
            return this.props.children
        }

        const content = (
            <>
                {this.state.error.message}
                <br />
                {this.state.showDetails && (
                    <>
                        <br />
                        <pre>{this.state.error.stack}</pre>
                        <br />
                    </>
                )}
                <Button
                    color="red"
                    onClick={() => this.setState({ showDetails: !this.state.showDetails })}
                >
                    {this.state.showDetails ? 'Hide' : 'Show'} details
                </Button>
                <Button color="grey" onClick={() => this.setState({ error: undefined })}>
                    Retry
                </Button>
            </>
        )

        if (this.props.showInAlert ?? true) {
            return (
                <Message error header={this.props.title || 'An error occurred'} content={content} />
            )
        }

        return (
            <p>
                {this.props.title || 'An error occurred'}: {content}
            </p>
        )
    }
}

export default ErrorBoundary
