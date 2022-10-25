import * as React from 'react'

const MuckTheDuck = (props: React.DetailedHTMLProps<React.ImgHTMLAttributes<HTMLImageElement>, HTMLImageElement>) => (
    <img
        src={require('./static/muck-the-duck.svg').default}
        alt='Muck the duck'
        {...props}
    />
)
export default MuckTheDuck
