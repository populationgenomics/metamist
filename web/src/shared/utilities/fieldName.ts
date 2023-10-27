const convertFieldName = (fieldName: string) => {
    return fieldName.replaceAll('_', ' ').split(' ').map((word) => {
        if (word === 'gcp') return word.toUpperCase()
        return word[0].toUpperCase() + word.slice(1)
    }).join(' ')
}

export { convertFieldName }
