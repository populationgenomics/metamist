WITH --  Get all sequencing groups with completed CRAMs
crams AS (
    SELECT
        DISTINCT asg.sequencing_group_id 'sg_id'
    FROM
        analysis a
        LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
    WHERE
        a.status = 'COMPLETED'
        AND a.type = 'CRAM'
),
-- Get the latest es-index for each project
es_index AS (
    SELECT
        a.project,
        a.id,
        a.sequencing_type,
        a.output,
        a.timestamp_completed
    FROM
        (
            -- nested query returns the latest es-index for each project and sequencing type combination at rn=1
            SELECT
                a.project,
                a.id,
                a.output,
                a.timestamp_completed,
                sg.type,
                ROW_NUMBER() OVER (
                    PARTITION BY a.project,
                    sg.type
                    ORDER BY
                        a.timestamp_completed DESC
                ) AS rn
            FROM
                analysis a
                LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
                LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
            WHERE
                a.status = 'COMPLETED'
                AND a.type = 'ES-INDEX'
        ) AS a
    WHERE
        a.rn = 1
),
-- Obtain the sgs in the latest es-index
es_index_sgs AS (
    SELECT
        asg.sequencing_group_id 'sg_id',
    FROM
        analysis_sequencing_group asg
        INNER JOIN es_index ON es_index.id = asg.analysis_id
),
joint_call AS (
    SELECT
        a.project,
        a.id,
        a.output,
        a.timestamp_completed
    FROM
        (
            SELECT
                a.project,
                a.id,
                a.output,
                a.timestamp_completed,
                sg.type,
                ROW_NUMBER() OVER (
                    PARTITION BY a.project,
                    sg.type
                    ORDER BY
                        a.timestamp_completed DESC
                ) AS rn
            FROM
                analysis a
                LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
                LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
            WHERE
                a.status = 'COMPLETED'
                AND a.type = 'CUSTOM'
                AND a.meta LIKE '%AnnotateDataset%'
        ) AS a
    WHERE
        a.rn = 1
),
joint_call_sgs AS (
    SELECT
        asg.sequencing_group_id 'sg_id',
    FROM
        analysis_sequencing_group asg
        INNER JOIN joint_call ON joint_call.id = asg.analysis_id
)
SELECT
    p.id 'project_id',
    p.name 'dataset',
    sg.type 'sequencing_type',
    sg.id 'sg_id',
    fam.id 'family_id',
    fam.external_id 'external_family_id',
    ppt.id 'participant_id',
    ppt.external_id 'external_participant_id',
    s.id 'sample_id',
    s.external_id 'external_sample_id',
    CASE WHEN crams.sg_id IS NOT NULL THEN 'YES' ELSE 'NO' END 'has_completed_cram',
    CASE WHEN es_index_sgs.sg_id IS NOT NULL THEN 'YES' ELSE 'NO' END 'in_latest_es_index',
    CASE WHEN joint_call_sgs.sg_id IS NOT NULL THEN 'YES' ELSE 'NO' END 'in_latest_joint_call'
FROM
    project p
    LEFT JOIN family fam ON fam.project = p.id
    LEFT JOIN family_participant ON family_participant.family_id = fam.id
    LEFT JOIN participant ppt ON ppt.id = family_participant.participant_id
    LEFT JOIN sample s ON s.participant_id = ppt.id
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
    LEFT JOIN crams ON crams.sg_id = sg.id
    LEFT JOIN es_index_sgs ON es_index_sgs.sg_id = sg.id
    LEFT JOIN joint_call_sgs ON joint_call_sgs.sg_id = sg.id;



SELECT
    f.project,
    sg.type 'sequencing_type',
    COUNT(DISTINCT f.id) 'num_families'
FROM
    family f
    LEFT JOIN family_participant fp ON f.id = fp.family_id
    LEFT JOIN sample s ON fp.participant_id = s.participant_id
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    f.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    f.project,
    sg.type;

SELECT
    p.project,
	sg.type 'sequencing_type',
    COUNT(DISTINCT p.id) 'num_participants'
FROM
    participant p
    LEFT JOIN sample s ON p.id = s.participant_id
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    f.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    p.project,
    sg.type;

SELECT
    s.project,
    sg.type 'sequencing_type',
    COUNT(DISTINCT s.id) 'num_samples'
FROM
    samples s
    LEFT JOIN sequencing_group sg on sg.sample_id = s.id
WHERE
    f.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    s.project,
    sg.type;

SELECT
    sg.project,
    sg.type 'sequencing_type',
    COUNT(DISTINCT sg.id) 'num_sgs'
FROM
    sequencing_group sg
WHERE
    sg.project IN :projects
    AND sg.type IN :sequencing_types
GROUP BY
    sg.project,
    sg.type;

SELECT
    a.project,
    sg.type 'sequencing_type',
    COUNT(DISTINCT asg.id) 'num_crams'
FROM
    analysis a
    LEFT JOIN analysis_sequencing_group asg ON a.id = asg.analysis_id
    LEFT JOIN sequencing_group sg ON sg.id = asg.sequencing_group_id
WHERE
    a.project IN :projects
    AND sg.type IN :sequencing_types
    AND a.type = 'CRAM'
    and a.status = 'COMPLETED'
GROUP BY
    a.project,
    sg.type