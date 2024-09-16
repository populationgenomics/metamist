const convertFieldName = (fieldName: string | undefined) => {
    if (!fieldName) return ''

    return fieldName
        .replaceAll('_', ' ')
        .replace('-', ' ')
        .split(' ')
        .map((word) => {
            if (word === 'gcp') return word.toUpperCase()
            return word[0].toUpperCase() + word.slice(1)
        })
        .join(' ')
}

export { convertFieldName }
