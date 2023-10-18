import * as React from 'react'

import { Checkbox, CheckboxProps } from 'semantic-ui-react'

import { FormEvent } from 'react';

interface SeqrProjectSelectorProps {
    projectNames: string[];
    projectIds: number[];
    onProjectSelected: (projectName: string, isSelected: boolean) => void;
  }

const SeqrProjectSelector = ({ projectNames, projectIds, onProjectSelected }: SeqrProjectSelectorProps) => {
    // State to keep track of selected projects
//   const [selectedProjects, setSelectedProjects] = React.useState<{ [key: string]: boolean }>({});

//   const handleCheckboxChange = (event: FormEvent<HTMLInputElement>, data: CheckboxProps) => {
//     const { name, checked } = data;
//     setSelectedProjects({
//       ...selectedProjects,
//       [name as string]: checked || false,
//     });
//   };
    const handleCheckboxChange = (event: FormEvent<HTMLInputElement>, data: CheckboxProps) => {
        const { name, checked } = data;
        onProjectSelected(name as string, checked || false);
    };

  return (
    <div>
      {projectNames.map((project: string, index: number) => (
        <div key={index}>
          <Checkbox
            label={project}
            name={project}
            onChange={(e, data) => handleCheckboxChange(e, data)}
          />
        </div>
      ))}
    </div>
  );
}

export default SeqrProjectSelector;
