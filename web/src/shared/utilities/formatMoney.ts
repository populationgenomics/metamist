const formatMoney = (val: number, dp: number = 2): string => `$${val.toFixed(dp).replace(/\d(?=(\d{3})+\.)/g, '$&,')}`

export default formatMoney
