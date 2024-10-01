const formatMoney = (val: number | undefined, dp: number = 2): string =>
    val ? `$${val.toFixed(dp).replace(/\B(?=(\d{3})+(?!\d))/g, ',')}` : ''

export default formatMoney
