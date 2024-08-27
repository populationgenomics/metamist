import { Modal, ModalProps } from 'semantic-ui-react'

export const SMModal: React.FC<ModalProps> = ({ title, children, style, ...props }) => {
    return (
        <Modal {...props} style={{ height: 'unset', top: '50px', left: 'unset', ...(style || {}) }}>
            {children}
        </Modal>
    )
}
