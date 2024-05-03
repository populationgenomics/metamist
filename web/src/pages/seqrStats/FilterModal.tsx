
import React, { useState } from 'react';
import { Button, Modal, Popup, Input, List, Checkbox } from 'semantic-ui-react';
import { FaFilter } from 'react-icons/fa';



interface FilterModalProps {
  columnName: string;
  options: string[];
  selectedOptions: string[];
  onSelectionChange: (selectedOptions: string[]) => void;
  onClose: () => void;
}

const FilterModal: React.FC<FilterModalProps> = ({
  columnName,
  options,
  selectedOptions,
  onSelectionChange,
  onClose,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const handleOpen = () => setIsOpen(true);
  const handleClose = () => setIsOpen(false);

  const handleSearch = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(event.target.value);
  };

  const handleCheckboxChange = (option: string) => {
    const updatedSelectedOptions = selectedOptions.includes(option)
      ? selectedOptions.filter((selectedOption) => selectedOption !== option)
      : [...selectedOptions, option];
    onSelectionChange(updatedSelectedOptions);
  };

  const filteredOptions = options.filter((option) =>
    option.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
  //   <Modal open onClose={onClose}>
  //     <Modal.Header>Filter by {columnName}</Modal.Header>
  //     <Modal.Content>
  //       <Input
  //         placeholder={`Search ${columnName}`}
  //         value={searchTerm}
  //         onChange={handleSearch}
  //       />
  //       <List divided relaxed>
  //         {filteredOptions.map((option) => (
  //           <List.Item key={option}>
  //             <Checkbox
  //               label={option}
  //               checked={selectedOptions.includes(option)}
  //               onChange={() => handleCheckboxChange(option)}
  //             />
  //           </List.Item>
  //         ))}
  //       </List>
  //     </Modal.Content>
  //     <Modal.Actions>
  //       <Button onClick={onClose}>Close</Button>
  //     </Modal.Actions>
  //   </Modal>
  // );
    <Popup
    trigger={
      <Button icon basic size="mini" onClick={handleOpen}>
        <FaFilter />
      </Button>
    }
    on="click"
    open={isOpen}
    onClose={handleClose}
    position="bottom left"
    className="filter-popup"
  >
    <div className="filter-popup-content">
      <Input
        placeholder={`Search ${columnName}`}
        value={searchTerm}
        onChange={handleSearch}
      />
      <List divided relaxed>
        {filteredOptions.map((option) => (
          <List.Item key={option}>
            <Checkbox
              label={option}
              checked={selectedOptions.includes(option)}
              onChange={() => handleCheckboxChange(option)}
            />
          </List.Item>
        ))}
      </List>
    </div>
  </Popup>
  );
};

export default FilterModal;