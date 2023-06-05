import * as React from 'react'
import { useNavigate } from 'react-router-dom'

import { Popup, Button, Grid } from 'semantic-ui-react'

import Diversity3RoundedIcon from '@mui/icons-material/Diversity3Rounded'
import { ThemeContext } from '../../shared/components/ThemeProvider'

const FamilyViewTitle: React.FunctionComponent<{
    externalId: string
    families: { id: number; externalId: string; participants: { id: number }[] }[]
    projectName: string
}> = ({ externalId, families, projectName }) => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'
    const navigate = useNavigate()

    return (
        <div
            style={{
                borderBottom: `1px solid  ${isDarkMode ? 'white' : 'black'}`,
            }}
        >
            <h1
                style={{
                    display: 'inline',
                }}
            >
                {externalId}
            </h1>
            {families.length > 1 && (
                <div style={{ float: 'right' }}>
                    {`Change Families within ${projectName}\t`}
                    <Popup
                        trigger={
                            <Diversity3RoundedIcon
                                sx={{
                                    fontSize: 40,
                                }}
                            />
                        }
                        hoverable
                        style={{ float: 'right' }}
                        position="bottom left"
                    >
                        <Grid
                            divided
                            centered
                            rows={3}
                            style={{
                                overflow: 'auto',
                                height: '400px',
                            }}
                        >
                            {families.map((item) => (
                                <Grid.Row key={item.id} textAlign="center">
                                    <Button
                                        onClick={() => {
                                            navigate(`/family/${item.id}`)
                                        }}
                                    >
                                        {item.externalId}
                                    </Button>
                                </Grid.Row>
                            ))}
                        </Grid>
                    </Popup>
                </div>
            )}
        </div>
    )
}

export default FamilyViewTitle
