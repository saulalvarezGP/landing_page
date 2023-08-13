function display_elements(){
    var ls_categories = document.getElementsByClassName('ant-typography ant-typography-ellipsis ant-typography-single-line w-full')
    var ls_omit = [...ls_categories]
        .filter(d=>d.innerText!='Logical Data Entities')
        .map(d=>d.innerText)
        
    [...ls_categories].filter(d=>d.innerText==='Logical Data Entities')[0].click()
    ls_omit.push('Logical Data Entities')
    
    var ls_categories2 = document.getElementsByClassName('ant-typography ant-typography-ellipsis ant-typography-single-line w-full')
    var ls_categories3 = [...ls_categories2]
        .filter(d=>!ls_omit.includes(d.innerText))
        
    [...ls_categories2].filter(d=>!ls_omit.includes(d.innerText)).map(d=>d.click())
    var path = 'M4.5 2C3.67157 2 3 2.67157 3 3.5V12.5C3 13.3284 3.67157 14 4.5 14H11.5C12.3284 14 13 13.3284 13 12.5V7.40312C13 7.26786 12.9922 7.13327 12.9767 7H11.5C10.5717 7 9.6815 6.63125 9.02513 5.97487C8.36875 5.3185 8 4.42826 8 3.5V2.02329C7.86673 2.00784 7.73214 2 7.59688 2H4.5ZM9 2.29356V3.5C9 4.16304 9.26339 4.79893 9.73223 5.26777C10.2011 5.73661 10.837 6 11.5 6H12.7064C12.5844 5.72117 12.4258 5.45759 12.233 5.21669L11.6649 4.50645C11.3189 4.07397 10.926 3.68114 10.4935 3.33515L9.78331 2.76696C9.54241 2.57424 9.27883 2.41557 9 2.29356ZM2 3.5C2 2.11929 3.11929 1 4.5 1H7.59688C8.6187 1 9.61009 1.34776 10.408 1.98609L11.1182 2.55428C11.6084 2.9464 12.0536 3.39161 12.4457 3.88176L13.0139 4.592C13.6522 5.3899 14 6.3813 14 7.40312V12.5C14 13.8807 12.8807 15 11.5 15H4.5C3.11929 15 2 13.8807 2 12.5V3.5Z'
    var ls_subcategories = document.getElementsByTagName('path')
    [...ls_subcategories].filter(e=>e.attributes.d.nodeValue===path)
}

function main(){
    display_elements()

}