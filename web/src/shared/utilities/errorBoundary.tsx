import React, { Component, ReactNode } from 'react'

interface Props {
    children?: ReactNode
}

interface State {
    error?: Error
}

class ErrorBoundary extends Component<Props, State> {
    public state: State = {}

    public static getDerivedStateFromError(error: Error): { error: Error } {
        // Update state so the next render will show the fallback UI.
        return { error }
    }

    public render(): React.ReactNode {
        if (this.state.error) {
            return <p>{this.state.error.toString()}</p>
        }

        return this.props.children
    }
}

export default ErrorBoundary
