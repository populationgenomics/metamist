import React, { useState } from 'react'
import { FaFilter } from 'react-icons/fa'
import { Button, Checkbox, Input, List, Popup } from 'semantic-ui-react'

interface FilterModalProps {
    columnName: string
    options: string[]
    selectedOptions: string[]
    onSelectionChange: (selectedOptions: string[]) => void
    onClose: () => void
}

interface FilterButtonProps {
    columnName: string
    options: string[]
    selectedOptions: string[]
    onSelectionChange: (selectedOptions: string[]) => void
}

const FilterModal: React.FC<FilterModalProps> = ({
    columnName,
    options,
    selectedOptions,
    onSelectionChange,
}) => {
    const [searchTerm, setSearchTerm] = useState('')

    const handleSearch = (event: React.ChangeEvent<HTMLInputElement>) => {
        setSearchTerm(event.target.value)
    }
    const handlePopupClick = (event: React.MouseEvent<HTMLDivElement>) => {
        event.stopPropagation() // Avoid sorting the table when clicking inside the popup
    }

    const handleCheckboxChange = (event: React.FormEvent<HTMLInputElement>, option: string) => {
        event.stopPropagation()
        const updatedSelectedOptions = selectedOptions.includes(option)
            ? selectedOptions.filter((selectedOption) => selectedOption !== option)
            : [...selectedOptions, option]
        onSelectionChange(updatedSelectedOptions)
    }

    const filteredOptions = options.filter((option) =>
        option.toLowerCase().includes(searchTerm.toLowerCase())
    )

    return (
        <div className="filter-popup-content" onClick={handlePopupClick}>
            <Input
                style={{ marginBottom: '10px' }}
                placeholder={`Search ${columnName}`}
                value={searchTerm}
                onChange={handleSearch}
            />
            <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                <List divided relaxed>
                    {filteredOptions.map((option) => (
                        <List.Item key={option}>
                            <Checkbox
                                label={option}
                                checked={selectedOptions.includes(option)}
                                onChange={(event) => handleCheckboxChange(event, option)}
                            />
                        </List.Item>
                    ))}
                </List>
            </div>
        </div>
    )
}

const FilterButton = ({
    columnName,
    options,
    selectedOptions,
    onSelectionChange,
}: FilterButtonProps) => {
    const [isOpen, setIsOpen] = useState(false)

    const handleOpen = (event: React.MouseEvent<HTMLButtonElement>) => {
        event.stopPropagation()
        setIsOpen(true)
    }

    const handleClose = () => {
        setIsOpen(false)
    }

    return (
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
            <FilterModal
                columnName={columnName}
                options={options}
                selectedOptions={selectedOptions}
                onSelectionChange={onSelectionChange}
                onClose={handleClose}
            />
        </Popup>
    )
}

export default FilterButton
