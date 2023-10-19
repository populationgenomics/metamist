import { Link } from 'react-router-dom'
import './NavDropdown.css'

interface ISubmenu {
    title: string
    url: string
}

const Dropdown: React.FunctionComponent = ({ submenus: List<ISubmenu> }) => (
    <div className="dropdown">
        <ul className="dropdown-menu">
            {submenus.map((submenu, index) => (
                <li key={index}>
                    <Link to={submenu.url}>{submenu.title}</Link>
                </li>
            ))}
        </ul>
    </div>
)

export default Dropdown;
