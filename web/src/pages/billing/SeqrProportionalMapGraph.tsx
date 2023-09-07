import * as React from 'react'
import _ from 'lodash'

import {
    scaleLinear,
    extent,
    stack,
    area,
    stackOffsetExpand,
    scaleTime,
    utcDay,
    utcMonth,
    select,
    pointer,
    interpolateRainbow,
    selectAll,
} from 'd3'

import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import { AnalysisApi, Project, ProjectApi } from '../../sm-api'

interface IProportionalDateProjectModel {
    project: string
    percentage: number
    size: number
}

interface IProportionalDateModel {
    date: string
    projects: IProportionalDateProjectModel[]
}

interface ISeqrProportionalMapGraphProps {
    start: string
    end: string
}

interface IPropMapData {
    date: Date
    [project: string]: number
}

const SeqrProportionalMapGraph: React.FunctionComponent<ISeqrProportionalMapGraphProps> = ({
    start,
    end,
}) => {
    const tooltipRef = React.useRef()
    const [hovered, setHovered] = React.useState('')
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [showProjectSelector, setShowProjectSelector] = React.useState<boolean>(false)
    const [sequencingType, setSequencingType] = React.useState<string>('genome')
    const [projectSelections, setProjectSelections] = React.useState<
        { [key: string]: boolean } | undefined
    >()
    const [data, setData] = React.useState<IPropMapData[]>()
    // const [propMap, setPropMap] = React.useState<>()

    function updateProjects(projects: { [key: string]: boolean }) {
        const updatedProjects = { ...(projectSelections || {}), ...projects }
        setProjectSelections(updatedProjects)
        loadPropMap(updatedProjects)
    }

    function loadPropMap(projects: { [key: string]: boolean } = {}) {
        const projectsToSearch = Object.keys(projects).filter((project) => projects[project])

        const api = new AnalysisApi()
        const defaultPropMap = projectsToSearch.reduce((prev, p) => ({ ...prev, [p]: 0 }), {})
        api.getProportionateMap(sequencingType, projectsToSearch, start, end)
            .then((summary) => {
                setIsLoading(false)

                const graphData: IPropMapData[] = summary.data.map(
                    (obj: IProportionalDateModel) => ({
                        date: new Date(obj.date),
                        ...defaultPropMap,
                        ...obj.projects.reduce(
                            (prev: { [project: string]: number }, projectObj) => ({
                                ...prev,
                                // in percentage, rounded to 2 decimal places
                                [projectObj.project]: projectObj.percentage,
                            }),
                            {}
                        ),
                    })
                )
                const projectsToSee = new Set(projectsToSearch)
                for (let index = 1; index < graphData.length; index++) {
                    const graphObj = graphData[index]
                    if (projectsToSearch.length == 0) continue
                    // make sure the entry BEFORE a project is visible,
                    // it's set to 0 to make the graph prettier
                    for (const project of projectsToSee) {
                        if (project in graphObj) {
                            projectsToSee.delete(project)
                            graphData[index - 1][project] = 0
                        }
                        if (projectsToSee.size == 0) break
                    }
                }
                setData(graphData)
            })
            .catch((er) => {
                setError(er.message)
                setIsLoading(false)
            })
    }

    function getSeqrProjects() {
        const api = new ProjectApi()
        api.getSeqrProjects()
            .then((projects) => {
                const newProjects = projects.data.reduce(
                    (prev: { [project: string]: boolean }, project: Project) => ({
                        ...prev,
                        [project.name!]: true,
                    }),
                    {}
                )
                updateProjects(newProjects)
            })
            .catch((er) => {
                setError(er.message)
                setIsLoading(false)
            })
    }

    // TODO, uncomment later
    // React.useEffect(() => {
    //     getSeqrProjects()
    // }, [])

    React.useEffect(() => {
        setProjectSelections(
            [
                'acute-care',
                'seqr',
                'perth-neuro',
                'rdnow',
                'ravenscroft-rdstudy',
                'heartkids',
                'circa',
                'hereditary-neuro',
                'schr-neuro',
                'ravenscroft-arch',
                'leukodystrophies',
                'brain-malf',
                'mito-disease',
                'epileptic-enceph',
                'ohmr3-mendelian',
                'ohmr4-epilepsy',
                'validation',
                'flinders-ophthal',
                'ibmdx',
                'mcri-lrp',
                'mcri-lrp-test',
                'kidgen',
                'ag-hidden',
                'udn-aus',
                'udn-aus-training',
                'rdp-kidney',
                'broad-rgp',
                'genomic-autopsy',
                'mito-mdt',
                'ag-cardiac',
                'ag-very-hidden',
            ].reduce(
                (prev: { [project: string]: boolean }, project: string) => ({
                    ...prev,
                    [project]: true,
                }),
                {}
            )
        )

        setData(
            [
                {
                    date: '2021-09-09T00:00:00.000Z',
                    'acute-care': 0,
                    seqr: 0,
                    'perth-neuro': 0,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0,
                    heartkids: 0,
                    circa: 0,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2021-12-02T00:00:00.000Z',
                    'acute-care': 0,
                    seqr: 0,
                    'perth-neuro': 1,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0,
                    heartkids: 0,
                    circa: 0,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2021-12-20T00:00:00.000Z',
                    'acute-care': 0.8394957470131452,
                    seqr: 0,
                    'perth-neuro': 0.16050425298685475,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0,
                    heartkids: 0,
                    circa: 0,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-01-05T00:00:00.000Z',
                    'acute-care': 0.8711886507709348,
                    seqr: 0,
                    'perth-neuro': 0.12881134922906526,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0,
                    heartkids: 0,
                    circa: 0,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-01-19T00:00:00.000Z',
                    'acute-care': 0.7573284153754336,
                    seqr: 0,
                    'perth-neuro': 0.11197631524204676,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.06093654910991636,
                    heartkids: 0.06975872027260333,
                    circa: 0,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-02-28T00:00:00.000Z',
                    'acute-care': 0.6588556712356408,
                    seqr: 0,
                    'perth-neuro': 0.09741642970668021,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.053013184440853976,
                    heartkids: 0.06068823978691063,
                    circa: 0.1300264748299144,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-03-09T00:00:00.000Z',
                    'acute-care': 0.6253619629746314,
                    seqr: 0,
                    'perth-neuro': 0.10039981305172946,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.0932187177178165,
                    heartkids: 0.05760308125061873,
                    circa: 0.12341642500520394,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-04-05T00:00:00.000Z',
                    'acute-care': 0.6737282231945022,
                    seqr: 0,
                    'perth-neuro': 0.08743806596741642,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.08118405942657285,
                    heartkids: 0.0501664481757846,
                    circa: 0.1074832032357239,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-05-03T00:00:00.000Z',
                    'acute-care': 0.5782007236087593,
                    seqr: 0,
                    'perth-neuro': 0.0599714977762652,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05568203717112424,
                    heartkids: 0.03440786346233139,
                    circa: 0.07371993664909213,
                    'hereditary-neuro': 0,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.1980179413324277,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-05-06T00:00:00.000Z',
                    'acute-care': 0.591877708315424,
                    seqr: 0,
                    'perth-neuro': 0.05055261910563999,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.04693684367596316,
                    heartkids: 0.02900390486059376,
                    circa: 0.062141784282539414,
                    'hereditary-neuro': 0.05256908809458764,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.16691805166525206,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-05-11T00:00:00.000Z',
                    'acute-care': 0.5620643602814317,
                    seqr: 0,
                    'perth-neuro': 0.04800624372056955,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.04457259775747548,
                    heartkids: 0.027542955245824917,
                    circa: 0.05901165348653163,
                    'hereditary-neuro': 0.0499211415725701,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0.05037079047772511,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.15851025745787156,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-05-12T00:00:00.000Z',
                    'acute-care': 0.5540090494347523,
                    seqr: 0,
                    'perth-neuro': 0.04731823493887593,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05826545085466399,
                    heartkids: 0.027148219194547435,
                    circa: 0.05816591900129844,
                    'hereditary-neuro': 0.04920568914113184,
                    'schr-neuro': 0,
                    'ravenscroft-arch': 0.04964889383462917,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.1562385436001009,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-05-26T00:00:00.000Z',
                    'acute-care': 0.501140818807358,
                    seqr: 0,
                    'perth-neuro': 0.04280272863048241,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05270526858591519,
                    heartkids: 0.024557506434593816,
                    circa: 0.052615234904075665,
                    'hereditary-neuro': 0.04451006598416821,
                    'schr-neuro': 0.013267160144907092,
                    'ravenscroft-arch': 0.044910976336129296,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.1413289399314136,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.08216130024095677,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-05-30T00:00:00.000Z',
                    'acute-care': 0.49678182287966977,
                    seqr: 0,
                    'perth-neuro': 0.05112857009321477,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05224683047328955,
                    heartkids: 0.02434390166219207,
                    circa: 0.05215757991754505,
                    'hereditary-neuro': 0.044122910938949864,
                    'schr-neuro': 0.013151760451101932,
                    'ravenscroft-arch': 0.04452033411420137,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.140099640200668,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.08144664926916761,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-06-16T00:00:00.000Z',
                    'acute-care': 0.49510556352973795,
                    seqr: 0,
                    'perth-neuro': 0.05433028668792545,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05207053731188221,
                    heartkids: 0.02426175958111002,
                    circa: 0.05198158790861887,
                    'hereditary-neuro': 0.04397402980320521,
                    'schr-neuro': 0.013107383301196502,
                    'ravenscroft-arch': 0.04437011197868033,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.13962691088370718,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.08117182901393631,
                    'ohmr4-epilepsy': 0,
                    validation: 0,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-06-20T00:00:00.000Z',
                    'acute-care': 0.4931881432885038,
                    seqr: 0,
                    'perth-neuro': 0.05411987905149172,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.051868881120620804,
                    heartkids: 0.024167799843358085,
                    circa: 0.0517802761961895,
                    'hereditary-neuro': 0.04380372936417927,
                    'schr-neuro': 0.013056621678014258,
                    'ravenscroft-arch': 0.04419827761227326,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.13908617071661888,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.08085747078523915,
                    'ohmr4-epilepsy': 0,
                    validation: 0.003872750343511255,
                    'flinders-ophthal': 0,
                    ibmdx: 0,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-06-27T00:00:00.000Z',
                    'acute-care': 0.49189892072607444,
                    seqr: 0,
                    'perth-neuro': 0.053978406532132484,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05173329284109975,
                    heartkids: 0.02410462380543759,
                    circa: 0.05164491953510712,
                    'hereditary-neuro': 0.043689223861597036,
                    'schr-neuro': 0.013022490907667347,
                    'ravenscroft-arch': 0.04408274073756594,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.1387225913567946,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.0806461046421234,
                    'ohmr4-epilepsy': 0,
                    validation: 0.0038626267483083793,
                    'flinders-ophthal': 0,
                    ibmdx: 0.0026140583060919307,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-07-05T00:00:00.000Z',
                    'acute-care': 0.4805484736367208,
                    seqr: 0,
                    'perth-neuro': 0.05273286802514362,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.050539559782518936,
                    heartkids: 0.023548415516327005,
                    circa: 0.050453225668912215,
                    'hereditary-neuro': 0.042681105724065825,
                    'schr-neuro': 0.012722000120249297,
                    'ravenscroft-arch': 0.04306554229452632,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.13552160154577983,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.07878521553435357,
                    'ohmr4-epilepsy': 0.023074755017961072,
                    validation: 0.0037734975823653398,
                    'flinders-ophthal': 0,
                    ibmdx: 0.002553739551076193,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-07-11T00:00:00.000Z',
                    'acute-care': 0.47419987079845544,
                    seqr: 0,
                    'perth-neuro': 0.05203620566123944,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.04987187356503541,
                    heartkids: 0.023237313627994666,
                    circa: 0.06299784016110449,
                    'hereditary-neuro': 0.042117238801568944,
                    'schr-neuro': 0.012553927739412052,
                    'ravenscroft-arch': 0.042496596518935,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.1337312039658882,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.07774437143558972,
                    'ohmr4-epilepsy': 0.022769910734322683,
                    validation: 0.0037236453015323714,
                    'flinders-ophthal': 0,
                    ibmdx: 0.002520001688921606,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-07-13T00:00:00.000Z',
                    'acute-care': 0.47335298201594705,
                    seqr: 0,
                    'perth-neuro': 0.05194327252993218,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.049782805783946,
                    heartkids: 0.023195813363108383,
                    circa: 0.06288533029461116,
                    'hereditary-neuro': 0.04382795239592555,
                    'schr-neuro': 0.012531507276578466,
                    'ravenscroft-arch': 0.04242070048204644,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.13349236911272944,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.07760552526517407,
                    'ohmr4-epilepsy': 0.022729245219279114,
                    validation: 0.0037169951237695687,
                    'flinders-ophthal': 0,
                    ibmdx: 0.0025155011369525464,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0,
                    'ag-hidden': 0,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-07-15T00:00:00.000Z',
                    'acute-care': 0.3875394702475829,
                    seqr: 0,
                    'perth-neuro': 0.042526548018023407,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.04075774932014429,
                    heartkids: 0.01899067622731905,
                    circa: 0.051484935163874916,
                    'hereditary-neuro': 0.05390800442416914,
                    'schr-neuro': 0.01025968753948895,
                    'ravenscroft-arch': 0.034730309973604045,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.10929172092191253,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.06353652621235151,
                    'ohmr4-epilepsy': 0.018608691581264995,
                    validation: 0.003043146184573901,
                    'flinders-ophthal': 0,
                    ibmdx: 0.0020594693918901726,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.07212566438780121,
                    'ag-hidden': 0.09113740040599898,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-07-26T00:00:00.000Z',
                    'acute-care': 0.3849263472087208,
                    seqr: 0,
                    'perth-neuro': 0.04223979760697966,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.0472257821723853,
                    heartkids: 0.018862624822538334,
                    circa: 0.05113777963376828,
                    'hereditary-neuro': 0.05354451048573267,
                    'schr-neuro': 0.010190507938598653,
                    'ravenscroft-arch': 0.034496128476991926,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.10855478253028114,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.06310810853311824,
                    'ohmr4-epilepsy': 0.018483215844140678,
                    validation: 0.003022626686520051,
                    'flinders-ophthal': 0,
                    ibmdx: 0.002045582685299122,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.07163933138232802,
                    'ag-hidden': 0.09052287399259709,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-07-29T00:00:00.000Z',
                    'acute-care': 0.3829824483159925,
                    seqr: 0,
                    'perth-neuro': 0.0420264843422663,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.046987289415683066,
                    heartkids: 0.01876736754599083,
                    circa: 0.0508795310780965,
                    'hereditary-neuro': 0.05327410780896174,
                    'schr-neuro': 0.010139045321810201,
                    'ravenscroft-arch': 0.03432192116061543,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.10800657500151316,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.06278940916844943,
                    'ohmr4-epilepsy': 0.0183898746034747,
                    validation: 0.008057416396584804,
                    'flinders-ophthal': 0,
                    ibmdx: 0.0020352523819936403,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.07127754888040332,
                    'ag-hidden': 0.0900657285781644,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-08-10T00:00:00.000Z',
                    'acute-care': 0.3800962171098147,
                    seqr: 0,
                    'perth-neuro': 0.04924596154739894,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.046633183942699725,
                    heartkids: 0.018625932965614562,
                    circa: 0.05049609290488776,
                    'hereditary-neuro': 0.05287262363360172,
                    'schr-neuro': 0.0100626354781285,
                    'ravenscroft-arch': 0.03406326439880997,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.1071926161670745,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.06231621580683114,
                    'ohmr4-epilepsy': 0.01825128488430707,
                    validation: 0.007996694118717377,
                    'flinders-ophthal': 0,
                    ibmdx: 0.0020199143189487483,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.07074038722512886,
                    'ag-hidden': 0.08938697549803641,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-09-01T00:00:00.000Z',
                    'acute-care': 0.47612348788225195,
                    seqr: 0,
                    'perth-neuro': 0.041617430451955,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.0394093896942304,
                    heartkids: 0.015740650511070837,
                    circa: 0.04267390804303672,
                    'hereditary-neuro': 0.0446822981568939,
                    'schr-neuro': 0.0085038654747621,
                    'ravenscroft-arch': 0.0287866353410436,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.0905877580236212,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.052663014303693564,
                    'ohmr4-epilepsy': 0.01542403794066208,
                    validation: 0.0067579523452082095,
                    'flinders-ophthal': 0,
                    ibmdx: 0.001707015987632749,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.059782224835889373,
                    'ag-hidden': 0.07554033100804833,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-10-07T00:00:00.000Z',
                    'acute-care': 0.46541858951160386,
                    seqr: 0,
                    'perth-neuro': 0.04068172705824662,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05051215129580081,
                    heartkids: 0.015386746391031762,
                    circa: 0.04171445138881666,
                    'hereditary-neuro': 0.04367768596507774,
                    'schr-neuro': 0.008312669245250514,
                    'ravenscroft-arch': 0.028139412480584786,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.08855103274551951,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.05147896808383464,
                    'ohmr4-epilepsy': 0.015077252363344286,
                    validation: 0.006606010265284969,
                    'flinders-ophthal': 0,
                    ibmdx: 0.011269273521423944,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.058438114205922514,
                    'ag-hidden': 0.07473591547825739,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-10-26T00:00:00.000Z',
                    'acute-care': 0.4608332008936827,
                    seqr: 0,
                    'perth-neuro': 0.05013310521296731,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.05001449639151019,
                    heartkids: 0.015235153366261708,
                    circa: 0.04130347302458367,
                    'hereditary-neuro': 0.04324736545662627,
                    'schr-neuro': 0.008230771315514375,
                    'ravenscroft-arch': 0.027862177869395616,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.08767861186073587,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.05097178792465439,
                    'ohmr4-epilepsy': 0.014928708530041555,
                    validation: 0.006540926650313564,
                    'flinders-ophthal': 0,
                    ibmdx: 0.011158246588461075,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.05786237127306329,
                    'ag-hidden': 0.07399960364218842,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-10-27T00:00:00.000Z',
                    'acute-care': 0.45415993087939716,
                    seqr: 0,
                    'perth-neuro': 0.049407133761492174,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.049290242500076575,
                    heartkids: 0.015014534947439121,
                    circa: 0.0407053624121402,
                    'hereditary-neuro': 0.04262110557227142,
                    'schr-neuro': 0.013099548414856093,
                    'ravenscroft-arch': 0.027458709031325888,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.08640894845477955,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.050233671609960756,
                    'ohmr4-epilepsy': 0.018955751952508836,
                    validation: 0.006446208280203662,
                    'flinders-ophthal': 0,
                    ibmdx: 0.014445257293800744,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.05702447325177669,
                    'ag-hidden': 0.07472912163797113,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
                {
                    date: '2022-10-31T00:00:00.000Z',
                    'acute-care': 0.44609936634962055,
                    seqr: 0,
                    'perth-neuro': 0.04853024136558073,
                    rdnow: 0,
                    'ravenscroft-rdstudy': 0.04841542472478123,
                    heartkids: 0.014748052548619668,
                    circa: 0.039982911623063146,
                    'hereditary-neuro': 0.04186465360802505,
                    'schr-neuro': 0.01286705376235686,
                    'ravenscroft-arch': 0.026971363757112038,
                    leukodystrophies: 0,
                    'brain-malf': 0,
                    'mito-disease': 0.084875336928062,
                    'epileptic-enceph': 0,
                    'ohmr3-mendelian': 0.049342109576305214,
                    'ohmr4-epilepsy': 0.01861931967076223,
                    validation: 0.006331799072605029,
                    'flinders-ophthal': 0,
                    ibmdx: 0.014188878602839504,
                    'mcri-lrp': 0,
                    'mcri-lrp-test': 0,
                    kidgen: 0.056012386065810195,
                    'ag-hidden': 0.07340280712570221,
                    'udn-aus': 0,
                    'udn-aus-training': 0,
                    'rdp-kidney': 0.017748295218754373,
                    'broad-rgp': 0,
                    'genomic-autopsy': 0,
                    'mito-mdt': 0,
                    'ag-cardiac': 0,
                    'ag-very-hidden': 0,
                },
            ].map((entry) => ({ ...entry, date: new Date(entry.date) }))
        )
    }, [])

    if (!data) {
        return 'Loading'
    }

    const selectedProjects: string[] = projectSelections
        ? _.sortBy(Object.keys(projectSelections).filter((project) => projectSelections[project]))
        : []

    // svg sizing info
    const margin = { top: 10, right: 400, bottom: 100, left: 80 }
    const height = 1000
    const width = 2400
    const innerWidth = width - margin.left - margin.right
    const innerHeight = height - margin.top - margin.bottom
    const id = '1'

    // d3 function that turns the data into stacked proportions
    const stackedData = stack().offset(stackOffsetExpand).keys(selectedProjects)(data)

    // function for generating the x Axis
    // domain refers to the min and max of the data (in this case earliest and latest dates)
    // range refers to the min and max pixel positions on the screen
    // basically it is a mapping of pixel positions to data values
    const xScale = scaleTime()
        .domain(extent(data, (d) => d.date)) // date is a string, will this take a date object? Yes :)
        .range([0, width - margin.left - margin.right])

    // function for generating the y Axis
    // no domain needed as it defaults to [0, 1] which is appropriate for proportions
    const yScale = scaleLinear().range([height - margin.top - margin.bottom, 0])

    // function that assigns each category a colour
    // can fiddle with the schemeAccent parameter for different colour scales - see https://d3js.org/d3-scale-chromatic/categorical#schemeAccent
    // const colour = scaleOrdinal().domain(selectedProjects).range(schemeSet3)

    // function that takes the various stacked data info and generates an svg path element (magically)
    const areaGenerator = area()
        .x((d) => xScale(d.data.date))
        .y0((d) => yScale(d[0]))
        .y1((d) => yScale(d[1]))

    let interval = utcDay.every(10)
    // more than 3 months
    if (new Date(end).valueOf() - new Date(start).valueOf() > 1000 * 60 * 60 * 24 * 90) {
        interval = utcMonth.every(1)
    }

    const mouseover = (
        event: React.MouseEvent<SVGPathElement, MouseEvent>,
        prevProp: number,
        newProp: number,
        project: string
    ) => {
        const tooltipDiv = tooltipRef.current
        const pos = pointer(event)
        if (tooltipDiv) {
            select(tooltipDiv).transition().duration(200).style('opacity', 0.9)
            select(tooltipDiv)
                .html(
                    `<h4>${project}</h4><h6>${(prevProp * 100).toFixed(1)}% &#8594; ${(
                        newProp * 100
                    ).toFixed(1)}%</h6>
                `
                )
                .style('left', `${pos[0] + 95}px`)
                .style('top', `${pos[1] + 100}px`)
        }
    }

    const mouseout = () => {
        const tooltipDiv = tooltipRef.current
        if (tooltipDiv) {
            select(tooltipDiv).transition().duration(500).style('opacity', 0)
        }
    }

    return (
        data && (
            <>
                <div
                    className="tooltip"
                    ref={tooltipRef}
                    style={{
                        position: 'absolute',
                        textAlign: 'center',
                        padding: '2px',
                        font: '12px sans-serif',
                        background: 'lightsteelblue',
                        border: '0px',
                        borderRadius: '8px',
                        pointerEvents: 'none',
                        opacity: 0,
                    }}
                />
                <svg id={id} width={width} height={height}>
                    {/* transform and translate move the relative (0,0) so you can draw accurately. Consider svg as a cartesian plane with (0, 0) top left and positive directions left and down the page
                then to draw in svg you just need to give coordinates. We've specified the width and height above so this svg 'canvas' can be drawn on anywhere between pixel 0 and the max height and width pixels */}
                    <g transform={`translate(${margin.left}, ${margin.top})`}>
                        {/* x-axis */}
                        <g id={`${id}-x-axis`}>
                            {/* draws the little ticks marks off the x axis + labels 
                            xScale.ticks() generates a list of evenly spaces ticks from min to max domain
                            You can pass an argument to ticks() to specify number of ticks to generate 
                            Calling xScale(tick) turns a tick value into a pixel position to be drawn 
                            eg in the domain [2000, 2010] and range[0, 200] passing 2005 would be 50% of the way across the domain so 50% of the way between min and max specified pixel positions so it would draw at 100
                            */}
                            {xScale.ticks(interval).map((tick) => (
                                <g
                                    key={`x-tick-${tick.toString()}`}
                                    transform={`translate(${xScale(tick)}, ${innerHeight})`}
                                >
                                    <text
                                        y={8}
                                        transform="translate(0, 10)rotate(-45)"
                                        textAnchor="end"
                                        alignmentBaseline="middle"
                                        fontSize={14}
                                        cursor="help"
                                    >
                                        {`${tick.toLocaleString('en-us', {
                                            month: 'short',
                                            year: 'numeric',
                                        })}`}
                                        {/* change this for different date formats */}
                                    </text>
                                    <line y2={6} stroke="black" />
                                    {/* this is the tiny vertical tick line that getting drawn (6 pixels tall) */}
                                </g>
                            ))}
                        </g>

                        {/* y-axis (same as above) */}
                        <g id={`${id}-y-axis`}>
                            {yScale.ticks().map((tick) => (
                                <g key={tick} transform={`translate(0, ${yScale(tick)})`}>
                                    <text
                                        key={tick}
                                        textAnchor="end"
                                        alignmentBaseline="middle"
                                        fontSize={14}
                                        fontWeight={600}
                                        x={-8}
                                        y={3}
                                    >
                                        {tick * 100}%
                                    </text>
                                    <line x2={-3} stroke="black" />
                                </g>
                            ))}
                        </g>

                        {/* stacked areas */}
                        <g id={`${id}-stacked-areas`}>
                            {/* for each 'project', draws a path (using path function) and fills it a new colour (using colour function) */}
                            {stackedData.map((area, i) => (
                                <React.Fragment key={`bigArea-${i}`}>
                                    {area.map((region, j) => {
                                        // don't draw an extra area at the end
                                        if (j + 1 >= area.length) {
                                            return (
                                                <React.Fragment key={`${i}-${j}`}></React.Fragment>
                                            )
                                        }
                                        const areas = area.slice(j, j + 2)
                                        // don't draw empty areas
                                        if (
                                            areas[0][1] - areas[0][0] === 0 &&
                                            areas[1][1] - areas[1][0] === 0
                                        ) {
                                            return (
                                                <React.Fragment key={`${i}-${j}`}></React.Fragment>
                                            )
                                        }

                                        const colour = interpolateRainbow(
                                            i / selectedProjects.length
                                        )
                                        // const colour = colors[i]
                                        return (
                                            <path
                                                key={`${i}-${j}`}
                                                d={areaGenerator(areas)}
                                                style={{
                                                    fill: colour,
                                                    stroke: colour,
                                                }}
                                                onMouseEnter={(e) => {
                                                    select(e.target).style('opacity', 0.6)
                                                }}
                                                onMouseMove={(e) =>
                                                    mouseover(
                                                        e,
                                                        areas[0][1] - areas[0][0],
                                                        areas[1][1] - areas[1][0],
                                                        selectedProjects[i]
                                                    )
                                                }
                                                onMouseLeave={(e) => {
                                                    select(e.target).style('opacity', 1)
                                                    mouseout()
                                                }}
                                            />
                                        )
                                    })}
                                </React.Fragment>
                            ))}

                            {stackedData.map((area, i) => {
                                const projectStart = area.findIndex((p) => p[1] - p[0])
                                if (projectStart === -1) {
                                    return <React.Fragment key={`bigArea-${i}`}></React.Fragment>
                                }
                                return (
                                    <path
                                        key={`bigArea-${i}`}
                                        d={areaGenerator(
                                            area.slice(projectStart - 1, area.length + 1)
                                        )}
                                        style={{
                                            stroke:
                                                selectedProjects[i] === hovered ? 'black' : 'none',
                                            strokeWidth:
                                                selectedProjects[i] === hovered ? 2 : 'none',
                                            opacity: selectedProjects[i] === hovered ? 1 : 0,
                                            fill: 'none',
                                        }}
                                    />
                                )
                            })}
                        </g>

                        {/* draws the main x axis line */}
                        <line
                            y1={`${innerHeight}`}
                            y2={`${innerHeight}`}
                            x2={`${innerWidth}`}
                            stroke="black"
                        />

                        {/* draws the main y axis line */}
                        <line y2={`${innerHeight}`} stroke="black" />

                        {/* x-axis label */}
                        <g id={`${id}-x-axis-label`}>
                            <text
                                x={innerWidth / 2}
                                y={innerHeight + 80}
                                fontSize={20}
                                textAnchor="middle"
                            >
                                {'Date'}
                            </text>
                        </g>

                        {/* y-axis label */}
                        <g
                            id={`${id}-y-axis-label`}
                            transform={`rotate(-90) translate(-${innerHeight / 2}, -60)`}
                        >
                            <text textAnchor="middle" fontSize={20}>
                                {'Proportion'}
                            </text>
                        </g>
                    </g>
                    <g transform={`translate(${width - margin.right + 30}, ${margin.top + 30})`}>
                        <text fontSize={25}>Projects</text>
                        {selectedProjects.map((project, i) => (
                            <React.Fragment key={`${project}-key`}>
                                <circle
                                    cy={25 + i * 25}
                                    cx={10}
                                    r={10}
                                    fill={interpolateRainbow(i / selectedProjects.length)}
                                    onMouseEnter={() => {
                                        setHovered(project)
                                    }}
                                    onMouseLeave={() => {
                                        setHovered('')
                                    }}
                                />
                                <text key={`${project}-legend`} y={30 + i * 25} x={30}>
                                    {project}
                                </text>
                            </React.Fragment>
                        ))}
                    </g>
                </svg>
            </>
        )
    )
}

export default SeqrProportionalMapGraph
