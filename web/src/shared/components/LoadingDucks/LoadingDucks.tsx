import * as React from 'react'
import MuckTheDuck from '../MuckTheDuck'
import './LoadingDucks.css'

interface ILoadingDucksProps {
    height?: number
}

const LoadingDucks: React.FunctionComponent<ILoadingDucksProps> = ({ height }) => (
    <>
        <div style={{ textAlign: 'center', paddingTop: '20px' }}>
            <MuckTheDuck height={height || 28} className="loadingScreen loadingScreenBig" />
        </div>
        <h2 style={{ textAlign: 'center', paddingTop: '5px', marginTop: 0 }}>Loading...</h2>
    </>
)
export default LoadingDucks
