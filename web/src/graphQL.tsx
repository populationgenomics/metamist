import * as React from "react";
import { useQuery, gql } from "@apollo/client";

const GET_PROJECTS = gql`
    query getProjects {
        myProjects {
            id
        }
    }
`;
export const GraphQL: React.FunctionComponent<{}> = () => {
    const { loading, error, data } = useQuery(GET_PROJECTS);
    if (loading) return <>"Loading..."</>;
    if (error) return <>`Error! ${error.message}`</>;
    console.log(data.myProjects);
    return (
        <>
            {data.myProjects.map((item: { id: string }) => (
                <option key={item.id}>{item.id}</option>
            ))}
        </>
    );
};
