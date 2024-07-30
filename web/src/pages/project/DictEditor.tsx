import Editor from '@monaco-editor/react'
import * as yaml from 'js-yaml'
import * as React from 'react'
import { Button } from 'semantic-ui-react'
import { ThemeContext } from '../../shared/components/ThemeProvider'

type InputValue = null | string | number | boolean | InputValue[] | { [key: string]: InputValue }
export type DictEditorInput = { [key: string]: InputValue } | string

interface DictEditorProps {
    input: DictEditorInput
    readOnly?: boolean
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
        } catch (jsonErr) {
            throw yamlErr
        }
    }
}

export const DictEditor: React.FunctionComponent<DictEditorProps> = ({
    input,
    onChange,
    readOnly,
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
            const _ = parseString(value)
            setError(undefined)
        } catch (e: any) {
            setError(e.message)
        }
    }

    const submit = () => {
        try {
            const newJson = parseString(textValue)
            onChange?.(newJson)
        } catch (e: any) {
            setError(e.message)
        }
    }

    return (
        <div
            style={{
                border: !!error ? '3px solid var(--color-border-red)' : '1px solid #ccc',
            }}
        >
            <Editor
                value={textValue}
                height="200px"
                theme={theme.theme === 'dark-mode' ? 'vs-dark' : 'vs-light'}
                language="yaml"
                onChange={(value) => handleChange(value || '')}
                options={{
                    minimap: { enabled: false },
                    automaticLayout: true,
                    readOnly: readOnly,
                }}
            />
            {error && (
                <p>
                    <em style={{ color: 'var(--color-text-red)' }}>{error}</em>
                </p>
            )}
            {!readOnly && (
                <p>
                    <Button onClick={submit} disabled={!!error}>
                        Apply
                    </Button>
                </p>
            )}
        </div>
    )
}
