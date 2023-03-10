import * as React from 'react'
import { useNavigate } from 'react-router-dom'

import { Popup, Button, Grid } from 'semantic-ui-react'

import Diversity3RoundedIcon from '@mui/icons-material/Diversity3Rounded'

const FamilyViewTitle: React.FunctionComponent<{
    externalId: string
    families: { id: number; externalId: string }[]
    projectName: string
}> = ({ externalId, families, projectName }) => {
    const navigate = useNavigate()

    return (
        <div
            style={{
                borderBottom: `1px solid black`,
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
                    >
                        <Grid divided centered rows={3}>
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
