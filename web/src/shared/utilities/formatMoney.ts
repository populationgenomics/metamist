const formatMoney = (val: number): string => `$${val.toFixed(2).replace(/\d(?=(\d{3})+\.)/g, '$&,')}`

export default formatMoney
