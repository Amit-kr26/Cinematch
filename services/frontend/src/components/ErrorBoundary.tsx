import { Component, ReactNode } from 'react'

interface Props { children: ReactNode; fallback?: ReactNode; onDismiss?: () => void }
interface State { hasError: boolean; message: string }

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, message: '' }
  }

  static getDerivedStateFromError(err: unknown): State {
    return {
      hasError: true,
      message: err instanceof Error ? err.message : String(err),
    }
  }

  render() {
    if (this.state.hasError) {
      return this.props.fallback ?? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
          <div className="bg-navy-900 border border-red-500/40 rounded-xl p-6 max-w-sm mx-4 text-center">
            <p className="text-red-400 font-semibold mb-1">Something went wrong</p>
            <p className="text-slate-500 text-xs font-mono break-all">{this.state.message}</p>
            <button
              className="mt-4 px-4 py-2 text-sm bg-navy-700 hover:bg-navy-600 text-slate-200 rounded-lg transition-colors"
              onClick={() => (this.props.onDismiss ? this.props.onDismiss() : this.setState({ hasError: false, message: '' }))}
            >
              Dismiss
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
