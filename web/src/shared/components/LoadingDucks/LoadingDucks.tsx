import * as React from 'react'
import MuckTheDuck from '../MuckTheDuck'
import './LoadingDucks.css'

const LoadingDucks: React.FunctionComponent = () => (
    <>
        <h2 style={{ textAlign: 'center', paddingTop: '200px' }}>Loading...</h2>
        <div style={{ textAlign: 'center', paddingTop: '20px' }}>
            <MuckTheDuck height={28} className="loadingScreen loadingScreenBig" />
        </div>
    </>
)
export default LoadingDucks
