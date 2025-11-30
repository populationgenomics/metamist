import { Box, FormControlLabel, Switch } from '@mui/material'
import * as Plot from '@observablehq/plot'
import { useState } from 'react'
import Report from '../../components/Report'
import { ReportItemPlot } from '../../components/ReportItem'
import ReportRow from '../../components/ReportRow'

export default function ConsentAndChoices({ project }: { project: string }) {
    const [normalize, setNormalize] = useState(true)
    const [onlySamples, setOnlySamples] = useState(false)

    return (
        <Report>
            <ReportRow>
                <Box>
                    <FormControlLabel
                        control={
                            <Switch
                                checked={normalize}
                                onChange={(event) => setNormalize(event.target.checked)}
                            />
                        }
                        label="Show Percentages"
                    />
                    <FormControlLabel
                        control={
                            <Switch
                                checked={onlySamples}
                                onChange={(event) => setOnlySamples(event.target.checked)}
                            />
                        }
                        label="Only include participants with samples"
                    />
                </Box>
            </ReportRow>
            <ReportRow>
                <ReportItemPlot
                    height={1200}
                    flexGrow={1}
                    title={'Participant Choices'}
                    description={`
                        Breakdown of participant choices. N/A choices for cell use understanding are
                        sano participants who were not asked this question, partial values for cell
                        use are participants who only ticked one box. Frequency refers to the ratio
                        of participants when viewing percentages, and the absolute count when not.
                    `}
                    project={project}
                    query={`
                        with answers as (
                            select
                            participant_id,
                            unnest(meta_ancestry_participant_ancestry) as ancestry,
                            meta_consent_blood_consent as "Blood consent",
                            meta_consent_informed_consent as "Informed consent",
                            meta_choice_receive_genetic_info as "Receive genetic info",
                            meta_choice_family_receive_genetic_info as "Family receive genetic info",
                            meta_choice_recontact as "Recontact for future research",
                            meta_choice_general_updates as "Receive general updates",
                            meta_choice_data_linkage as "Data linkage",
                            meta_have_donated_either_blood_or_plasma "Have previously donated blood or plasma",
                            meta_choice_use_of_cells_in_future_research_consent "Use of cells in future research consent",
                            CASE
                                WHEN
                                    meta_choice_use_of_cells_in_future_research_understanding @> ['used_by_approved_researchers','grown_indefinitely']
                                    or meta_choice_use_of_cells_in_future_research_understanding @> ['grown_indefinitely_and_used_by_approved_researchers']
                                THEN 'yes'
                                WHEN len(meta_choice_use_of_cells_in_future_research_understanding) = 1 THEN 'partial'
                                WHEN external_id_sano is not null  THEN 'n/a' -- Sano participants weren't asked this question
                                ELSE 'no'
                            END as  "Use of cells in future research understanding"
                        from participant
                        ${onlySamples ? 'where exists (select 1 from sample where sample.participant_id = participant.participant_id)' : ''}
                        ), unpivoted as (
                            UNPIVOT answers
                            ON COLUMNS(* EXCLUDE (ancestry, participant_id))
                            INTO NAME question VALUE answer
                        ) select * from unpivoted where ancestry in ('Vietnamese', 'Filipino', 'Lebanese')
                    `}
                    plot={(data) => ({
                        marginLeft: 70,
                        marginRight: 120,
                        inset: 5,
                        x: { percent: normalize },
                        y: { label: null },
                        fy: {
                            label: null,
                            // A bit hacky, but seems to be one of the only ways to split a tick
                            // label on to multiple lines.
                            tickFormat: (value: string) =>
                                value
                                    .split(' ')
                                    .map((val, i) => (i % 2 === 0 ? `${val} ` : `${val}\n`))
                                    .join(''),
                            domain: [
                                'Informed consent',
                                'Blood consent',
                                'Receive general updates',
                                'Recontact for future research',
                                'Receive genetic info',
                                'Family receive genetic info',
                                'Use of cells in future research consent',
                                'Use of cells in future research understanding',
                                'Data linkage',
                                'Have previously donated blood or plasma',
                            ],
                        },

                        color: {
                            legend: true,
                            range: [
                                '#6cc5b0',
                                '#efb118',
                                '#97bbf5',
                                '#ff725c',
                                '#9c6b4e',
                                '#9498a0',
                            ],
                            domain: ['yes', 'partial', 'unsure', 'no', 'dont_know', 'n/a'],
                        },
                        marks: [
                            Plot.barX(
                                data,
                                Plot.groupY(
                                    { x: 'count' },
                                    {
                                        y: 'ancestry',
                                        fy: 'question',
                                        fill: 'answer',
                                        tip: true,
                                        order: '-sum',
                                        offset: normalize ? 'normalize' : undefined,
                                    }
                                )
                            ),
                            Plot.frame(),
                        ],
                    })}
                />
            </ReportRow>
        </Report>
    )
}
