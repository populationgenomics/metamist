import Editor from '@monaco-editor/react'
import * as yaml from 'js-yaml'
import * as React from 'react'
import { Button } from 'semantic-ui-react'
import { ThemeContext } from '../../shared/components/ThemeProvider'

type InputValue = null | string | number | boolean | InputValue[] | { [key: string]: InputValue }
export type DictEditorInput = { [key: string]: InputValue } | string

interface DictEditorProps {
    input: DictEditorInput
    height?: string
    readonly?: boolean
    onChange?: (json: object) => void
}

const getStringFromValue = (input: DictEditorInput) => {
    // if it's a string, return it
    if (typeof input === 'string') {
        return input
    }

    // otherwise try to convert it to a string
    return yaml.dump(input)
}

const parseString = (str: string) => {
    // try yaml, then json, if both fail, throw the yaml error
    if (str.trim().length === 0) {
        return {}
    }
    try {
        return yaml.load(str)
    } catch (yamlErr) {
        try {
            return JSON.parse(str)
        } catch (_jsonErr) {
            throw yamlErr
        }
    }
}

export const DictEditor: React.FunctionComponent<DictEditorProps> = ({
    input,
    onChange,
    height,
    readonly,
}) => {
    const [textValue, setInnerTextValue] = React.useState<string>(getStringFromValue(input))
    const theme = React.useContext(ThemeContext)

    const [error, setError] = React.useState<string | undefined>(undefined)

    React.useEffect(() => {
        setInnerTextValue(getStringFromValue(input))
    }, [input])

    const handleChange = (value: string) => {
        setInnerTextValue(value)
        try {
            parseString(value)
            setError(undefined)
        } catch (e) {
            const err = e as unknown as { message: string }
            setError(err.message)
        }
    }

    const submit = () => {
        try {
            const newJson = parseString(textValue)
            onChange?.(newJson)
        } catch (e) {
            const err = e as unknown as { message: string }
            setError(err.message)
        }
    }

    return (
        <div
            style={{
                border: error ? '3px solid var(--color-border-red)' : '1px solid #ccc',
            }}
        >
            <Editor
                value={textValue}
                height={height || '200px'}
                theme={theme.theme === 'dark-mode' ? 'vs-dark' : 'vs-light'}
                language="yaml"
                onChange={(value) => handleChange(value || '')}
                options={{
                    minimap: { enabled: false },
                    automaticLayout: true,
                    readOnly: readonly,
                    scrollBeyondLastLine: false,
                }}
            />
            {error && (
                <p>
                    <em style={{ color: 'var(--color-text-red)' }}>{error}</em>
                </p>
            )}
            {!readonly && (
                <p>
                    <Button onClick={submit} disabled={!!error}>
                        Apply
                    </Button>
                </p>
            )}
        </div>
    )
}
