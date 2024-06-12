import * as React from 'react'
import { Button } from 'semantic-ui-react'

interface JsonEditorProps {
    jsonStr?: string
    jsonObj?: any
    onChange: (json: object) => void
}

export const JsonEditor: React.FunctionComponent<JsonEditorProps> = ({
    jsonStr,
    jsonObj,
    onChange,
}) => {
    const [innerJsonValue, setInnerJsonValue] = React.useState<string>(
        jsonObj ? JSON.stringify(jsonObj, null, 2) : jsonStr || ''
    )
    const [error, setError] = React.useState<string | undefined>(undefined)

    React.useEffect(() => {
        if (jsonObj) {
            setInnerJsonValue(JSON.stringify(jsonObj, null, 2))
            return
        }
        if (jsonStr) {
            setInnerJsonValue(jsonStr)
        }
    }, [jsonStr, jsonObj])

    const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
        const value = e.target.value
        setInnerJsonValue(value)
        try {
            const newJson = JSON.parse(value)
            setError(undefined)
        } catch (e: any) {
            setError(e.message)
        }
    }

    const submit = () => {
        try {
            const newJson = JSON.parse(innerJsonValue)
            onChange(newJson)
        } catch (e: any) {
            setError(e.message)
        }
    }

    return (
        <div>
            <textarea
                value={innerJsonValue}
                onChange={handleChange}
                style={{
                    width: '100%',
                    height: '400px',
                    border: !!error ? '3px solid var(--color-border-red)' : '1px solid #ccc',
                    padding: '10px',
                    fontFamily:
                        'Consolas,Monaco,Lucida Console,Liberation Mono,DejaVu Sans Mono,Bitstream Vera Sans Mono,Courier New, monospace',
                }}
            />
            {error && (
                <p>
                    <em style={{ color: 'var(--color-text-red)' }}>{error}</em>
                </p>
            )}
            <p>
                <Button onClick={submit} disabled={!!error}>
                    Apply
                </Button>
            </p>
        </div>
    )
}
