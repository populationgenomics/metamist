import * as React from 'react'
import MuckTheDuck from '../MuckTheDuck'
import './LoadingDucks.css'

const LoadingDucks: React.FunctionComponent = () => (
    <div style={{ textAlign: 'center', paddingTop: '200px' }}>
        <MuckTheDuck height={28} className="loadingScreen" />
    </div>
)
export default LoadingDucks
