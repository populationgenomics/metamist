import React from 'react'
import { ProjectSeqrStats, WebApi, ProjectApi, EnumsApi } from '../../sm-api'
import SeqrProjectSelector from './SeqrProjectSelector'
import SequencingTypeSelector from './SequencingTypeSelector'
import { Table, Checkbox, CheckboxProps } from 'semantic-ui-react'

interface SeqrProjectProps {
    id: number
    name: string
}

interface SelectedProject {
    id: number;
    name: string;
}

function getPercentageColor(percentage: number) {
    const red = 265 - (percentage / 100 * 85); // Reducing intensity
    const green = 180 + (percentage / 100 * 85); // Reducing intensity
    const blue = 155; // Adding more blue for a pastel tone
    return `rgb(${red}, ${green}, ${blue})`;
}

const SeqrStats: React.FC = () => {
    // Get the list of seqr projects from the project API
    const [seqrProjectNames, setSeqrProjectNames] = React.useState<string[]>([])
    const [seqrProjectIds, setSeqrProjectIds] = React.useState<number[]>([])
    const [selectedProjects, setSelectedProjects] = React.useState<SelectedProject[]>([]);


    const handleProjectSelected = (projectName: string, isSelected: boolean) => {
        const projectIndex = seqrProjectNames.indexOf(projectName);
        const project = { id: seqrProjectIds[projectIndex], name: projectName };
    
        if (isSelected) {
            setSelectedProjects([...selectedProjects, project]);
        } else {
            const updatedSelectedProjects = selectedProjects.filter(p => p.name !== projectName);
            setSelectedProjects(updatedSelectedProjects);
    
            // If no projects are selected, reset the responses state to empty
            if (updatedSelectedProjects.length === 0) {
                setResponses([]);
            }
        }
        // Reset the page number when project selections change
        setPageNumber(0);
    };

    React.useEffect(() => {
        new ProjectApi().getSeqrProjects({}).then((resp) => {
            const projects: SeqrProjectProps[] = resp.data
            setSeqrProjectNames(projects.map((project) => project.name))
            setSeqrProjectIds(projects.map((project) => project.id))
        })
    }, [])

    


    const [responses, setResponses] = React.useState<ProjectSeqrStats[]>([])
    // 0-indexed page number
    const [pageNumber, setPageNumber] = React.useState<number>(0)
    const [pageSize, setPageSize] = React.useState<number>(10)

    const pagedResults = responses.slice(pageNumber * pageSize, (pageNumber + 1) * pageSize);

    const [hideFullPercentage, setHideFullPercentage] = React.useState(false);

    // New state for sequencing types
    const [seqTypes, setSeqTypes] = React.useState<string[]>([]);
    const [selectedSeqTypes, setSelectedSeqTypes] = React.useState<string[]>([]);

    const handleSeqTypeSelected = (seqType: string, isSelected: boolean) => {
        if (isSelected) {
            setSelectedSeqTypes([...selectedSeqTypes, seqType]);
        } else {
            setSelectedSeqTypes(selectedSeqTypes.filter(type => type !== seqType));
        }
        // Reset the page number when sequencing type selections change
        setPageNumber(0);
    };

    // Fetching Sequencing Types
    React.useEffect(() => {
        new EnumsApi().getSequencingTypes().then(resp => {
            setSeqTypes(resp.data);
        });
    }, []);

    // Fetching project stats
    React.useEffect(() => {
        if (selectedProjects.length > 0 && selectedSeqTypes.length > 0) {
            const projectIds = selectedProjects.map(p => p.id);
            new WebApi().getProjectsSeqrStats({ projects: projectIds, sequencing_types: selectedSeqTypes })
                .then(resp => {
                    setResponses(resp.data);
                });
        } else {
            setResponses([]);
        }
    }, [selectedProjects, selectedSeqTypes]);


    return (
        <div>
            <h1>Select projects</h1>
            <SeqrProjectSelector
                projectNames={seqrProjectNames}
                projectIds={seqrProjectIds}
                onProjectSelected={handleProjectSelected}
            />
            <h1>Select sequencing types</h1>
            <SequencingTypeSelector
                seqTypes={seqTypes}
                onSeqTypeSelected={handleSeqTypeSelected}
            />
            <h1>Hide full percentages</h1>
            <Checkbox
                label="Hide 100% Aligned"
                checked={hideFullPercentage}
                onChange={() => setHideFullPercentage(!hideFullPercentage)}
            />
            <h2>Page Size</h2>
            <select
                onChange={(e) => {
                    setPageSize(parseInt(e.currentTarget.value))
                    setPageNumber(0)
                }}
                value={pageSize}
            >
                {[1, 10, 50, 100].map((value) => (
                    <option value={value} key={`setPageSize-${value}`}>
                        {value}
                    </option>
                ))}
            </select>
            <Table>
                <thead>
                    <tr>
                        <th>Dataset</th>
                        <th>Sequencing Type</th>
                        <th>Families</th>
                        <th>Participants</th>
                        <th>Samples</th>
                        <th>Sequencing Groups</th>
                        <th>CRAMs</th>
                        <th>% Aligned</th>
                        <th>ES-Index Analysis ID</th>
                        <th>ES-Index SGs</th>
                        <th>% in Index</th>
                        <th>Joint Call Analysis ID</th>
                        <th>Joint Call SGs</th>
                        <th>% in Joint Call</th>
                    </tr>
                </thead>
                <tbody>
                    {pagedResults.map((ss) => {
                        const percentageAligned = ss.total_sequencing_groups > 0 
                            ? (ss.total_crams / ss.total_sequencing_groups) * 100 
                            : 0;

                        const percentageInIndex = ss.total_sgs_in_latest_es_index > 0
                            ? (ss.total_sgs_in_latest_es_index / ss.total_sequencing_groups) * 100
                            : 0;
                        
                        const percentageInJointCall = ss.total_sgs_in_latest_annotate_dataset > 0
                            ? (ss.total_sgs_in_latest_annotate_dataset / ss.total_sequencing_groups) * 100
                            : 0;
                        
                        const rowClass = ss.sequencing_type === 'exome' ? 'exome-row' : ss.sequencing_type === 'genome' ? 'genome-row' : '';
                                    
                        return (
                            <tr key={`${ss.project}-${ss.sequencing_type}`} className = {rowClass}>
                                <td>{ss.dataset}</td>
                                <td>{ss.sequencing_type}</td>
                                <td>{ss.total_families}</td>
                                <td>{ss.total_participants}</td>
                                <td>{ss.total_samples}</td>
                                <td>{ss.total_sequencing_groups}</td>
                                <td>{ss.total_crams}</td>
                                <td style={{ backgroundColor: getPercentageColor(percentageAligned) }}>
                                    {percentageAligned.toFixed(2)}%
                                </td>
                                <td>{ss.latest_es_index_id}</td>
                                <td>{ss.total_sgs_in_latest_es_index}</td>
                                <td style={{ backgroundColor: getPercentageColor(percentageInIndex) }}>
                                    {percentageInIndex.toFixed(2)}%
                                </td>
                                <td>{ss.latest_annotate_dataset_id}</td>
                                <td>{ss.total_sgs_in_latest_annotate_dataset}</td>
                                <td style={{ backgroundColor: getPercentageColor(percentageInJointCall) }}>
                                    {percentageInJointCall.toFixed(2)}%
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </Table>
            {
                <button disabled={pageNumber === 0} onClick={() => setPageNumber(pageNumber - 1)}>
                    Previous page
                </button>
            }
            Page {pageNumber + 1}
            {(pageNumber + 1) * pageSize < responses.length && (
                <button onClick={() => setPageNumber(pageNumber + 1)}>Next page</button>
            )}
        </div>
    )
}

export default SeqrStats
