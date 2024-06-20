// const divs = document.querySelectorAll('div[data-tb-test-id="user-profile-photo"]');

const rawdata = document.getElementsByClassName('shared-widgets-datagrid_bcell-wrapper_f13t8u3y');

[...rawdata].slice(0,-3).map(d=>d.innerText)


const numRows = ls.length
const indices = Array.from({ length: numRows }, (_, i) => i);
const result = indices.map(i => ls.slice(i * 6, i * 6 +6));
