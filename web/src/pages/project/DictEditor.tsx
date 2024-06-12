import Editor from '@monaco-editor/react'
import * as yaml from 'js-yaml'
import * as React from 'react'
import { Button } from 'semantic-ui-react'
import { ThemeContext } from '../../shared/components/ThemeProvider'

interface DictEditorProps {
    jsonStr?: string
    yamlStr?: string
    obj?: any
    onChange: (json: object) => void
}

const getStringFromValues = (obj?: any, jsonStr?: string, yamlString?: string) => {
    if (obj) {
        return yaml.dump(obj, { indent: 2 })
    }
    return jsonStr || yamlString || '\n'
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
    jsonStr,
    yamlStr,
    obj,
    onChange,
}) => {
    const [textValue, setInnerTextValue] = React.useState<string>(
        getStringFromValues(obj, jsonStr, yamlStr)
    )
    const theme = React.useContext(ThemeContext)

    const [error, setError] = React.useState<string | undefined>(undefined)

    React.useEffect(() => {
        setInnerTextValue(getStringFromValues(obj, jsonStr, yamlStr))
    }, [obj, jsonStr, yamlStr])

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
            onChange(newJson)
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
                }}

                // style={{
                //     width: '100%',
                //     height: '400px',
                //     border: !!error ? '3px solid var(--color-border-red)' : '1px solid #ccc',
                //     padding: '10px',
                //     fontFamily:
                //         'Consolas,Monaco,Lucida Console,Liberation Mono,DejaVu Sans Mono,Bitstream Vera Sans Mono,Courier New, monospace',
                // }}
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
