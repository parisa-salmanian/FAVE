// FAVE shell — builds the design's full UI (topbar, rail, popovers, AI
// sidebar, map overlays) at runtime and wires user actions back to the
// legacy form controls (citySelect, poi-check, etc.) which stay hidden in
// the DOM so existing fairness/POI/DR handlers keep firing untouched.

(function () {
  // ---------------- icon set ----------------
  const ICONS = {
    district: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M10.475 3.698V4.467L9.256 4.37V12.79H11.582L11.591 20.99L14.078 20.996V14.788H15.995L16.003 5.036L14.654 4.896L14.651 3ZM4.465 8.488L3.242 8.504L4.395 20.998L6.389 20.995V12.79H8.494L8.492 8.512L7.722 8.5V7.958L4.465 7.956ZM6.959 20.995H7.001H11.022V13.361L6.975 13.359ZM17.938 10.746L16.926 10.748V14.786L18.589 14.789V20.981L19.432 20.998L20.758 10.747L19.503 10.745L19.489 10.101L17.949 10.088ZM14.647 21L18.019 20.997V15.356H14.646Z"/>',
    hexagons: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M5.496 8.838L6.744 9.558L7.992 10.279V11.72V13.161L6.744 13.881L5.496 14.602L4.248 13.881L3 13.161V11.72V10.279L4.248 9.558ZM8.892 3.502L10.14 4.222L11.387 4.943V6.384V7.825L10.14 8.545L8.892 9.266L7.644 8.545L6.396 7.825V6.384V4.943L7.644 4.222ZM11.899 9.032L13.147 9.752L14.395 10.473V11.914V13.355L13.147 14.075L11.899 14.796L10.651 14.075L9.403 13.355V11.914V10.473L10.651 9.752ZM15.518 3.465L16.766 4.185L18.014 4.905V6.346V7.787L16.766 8.508L15.518 9.228L14.27 8.508L13.022 7.787V6.346V4.905L14.27 4.185ZM18.503 9.229L19.752 9.95L21 10.671V12.112V13.554L19.752 14.274L18.503 14.995L17.255 14.274L16.007 13.554V12.112V10.671L17.255 9.95ZM15.004 14.772L16.252 15.492L17.5 16.212V17.653V19.094L16.252 19.815L15.004 20.535L13.756 19.815L12.508 19.094V17.653V16.212L13.756 15.492Z"/>',
    building: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M10.66 15.107C10.895 15.232 11.065 15.337 11.355 15.405V16.397C11.065 16.33 10.895 16.224 10.66 16.1ZM9.668 14.71C9.976 14.782 9.955 14.837 10.263 14.909V15.901C9.955 15.829 9.976 15.774 9.668 15.702ZM8.576 14.214C8.866 14.282 9.036 14.387 9.271 14.512V15.405C8.981 15.337 8.811 15.232 8.576 15.107ZM10.66 13.222C10.95 13.289 11.12 13.395 11.355 13.519V14.412C10.938 14.378 11.028 14.3 10.66 14.214ZM9.668 12.825C9.909 12.881 10.066 12.928 10.263 13.023V14.015L9.67 13.714ZM8.576 12.229C8.811 12.354 8.981 12.459 9.271 12.527V13.519C9.036 13.395 8.866 13.289 8.576 13.222ZM10.66 11.336C10.95 11.404 11.12 11.509 11.355 11.634V12.527C11.065 12.459 10.895 12.354 10.66 12.229ZM9.668 10.84L10.261 11.142L10.263 12.031C9.955 11.959 9.976 11.904 9.668 11.832ZM8.576 10.344C8.866 10.411 9.036 10.517 9.271 10.641V11.535L8.581 11.322ZM10.66 9.351C10.895 9.476 11.065 9.581 11.355 9.649V10.641C11.12 10.517 10.95 10.411 10.66 10.344ZM9.668 8.954C9.976 9.026 9.955 9.081 10.263 9.153V10.145C10.031 10.022 10.005 9.975 9.668 9.947ZM8.576 8.359C8.811 8.483 8.981 8.589 9.271 8.657V9.649C8.981 9.581 8.811 9.476 8.576 9.351ZM10.66 7.466C10.95 7.533 11.12 7.639 11.355 7.763V8.657L10.665 8.444ZM9.668 6.969L10.261 7.271L10.263 8.26L9.67 7.958ZM8.576 6.473C8.816 6.588 9.028 6.643 9.271 6.771V7.763C9.036 7.639 8.866 7.533 8.576 7.466ZM8.08 19.077L11.931 21L11.95 7.069C11.38 6.687 10.696 6.392 10.065 6.076C9.631 5.859 8.559 5.223 8.08 5.183ZM12.744 16.397V15.405C13.034 15.337 13.204 15.232 13.439 15.107V16.1C13.149 16.167 12.979 16.273 12.744 16.397ZM13.737 15.901V15.008C13.972 14.883 14.141 14.778 14.431 14.71V15.702C14.018 15.737 14.103 15.813 13.737 15.901ZM14.828 15.405V14.412C15.136 14.341 15.116 14.286 15.424 14.214V15.206C15.117 15.278 15.136 15.331 14.828 15.405ZM12.744 14.412V13.519C12.979 13.395 13.149 13.289 13.439 13.222V14.214C13.071 14.3 13.161 14.378 12.744 14.412ZM13.737 14.015V13.023C14.027 12.956 14.196 12.85 14.431 12.725V13.718C14.196 13.842 14.027 13.948 13.737 14.015ZM14.83 13.523L14.828 12.527C15.026 12.432 15.183 12.384 15.424 12.328V13.222ZM12.744 12.527V11.535C13.034 11.467 13.204 11.361 13.439 11.237V12.229C13.204 12.354 13.034 12.459 12.744 12.527ZM13.737 12.031V11.138C13.972 11.013 14.141 10.907 14.431 10.84V11.832C14.064 11.918 14.154 11.996 13.737 12.031ZM14.828 11.535V10.641L15.422 10.34L15.424 11.336C15.117 11.407 15.136 11.461 14.828 11.535ZM12.744 10.641V9.649C12.987 9.521 13.199 9.466 13.439 9.351V10.344C13.149 10.411 12.979 10.517 12.744 10.641ZM13.737 10.145V9.252C13.972 9.128 14.141 9.022 14.431 8.954V9.847C14.196 9.972 14.027 10.078 13.737 10.145ZM14.83 9.653L14.828 8.657C15.136 8.585 15.116 8.53 15.424 8.458V9.351ZM12.744 8.657V7.763C12.979 7.639 13.149 7.533 13.439 7.466V8.359C13.204 8.483 13.034 8.589 12.744 8.657ZM13.737 8.26V7.267C14.027 7.2 14.196 7.094 14.431 6.969V7.962C14.141 8.029 13.972 8.135 13.737 8.26ZM14.828 7.664V6.771L15.422 6.469C15.424 7.273 15.62 7.598 14.828 7.664ZM12.167 7.006L12.149 20.962L13.638 20.269V18.382C14.013 18.202 14.359 18.081 14.729 17.886V19.672C15.212 19.56 15.437 19.288 15.92 19.176V5.183ZM8.179 4.786C8.464 5.175 12.049 6.726 12.05 6.726C12.264 6.726 13.617 5.988 13.968 5.812C14.54 5.526 15.718 5.062 15.92 4.786C15.415 4.669 12.375 3 12.05 3Z"/>',
    layers: '<path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5" opacity="0.5"/><path d="M2 12l10 5 10-5" opacity="0.7"/>',
    sparkle: '<path d="M12 3l1.7 5.3L19 10l-5.3 1.7L12 17l-1.7-5.3L5 10l5.3-1.7z"/>',
    wand: '<path d="M15 4V2M15 16v-2M8 9h2M20 9h2M17.8 11.8l1.4 1.4M17.8 6.2l1.4-1.4"/><path d="M3 21l9-9 3 3-9 9z"/>',
    parallel: '<path d="M4 4v16M9 4v16M14 4v16M19 4v16" opacity="0.4"/><path d="M4 8 L9 14 L14 6 L19 11"/>',
    scatter: '<circle cx="6" cy="17" r="1.5" fill="currentColor"/><circle cx="10" cy="12" r="1.5" fill="currentColor"/><circle cx="14" cy="14" r="1.5" fill="currentColor"/><circle cx="17" cy="8" r="1.5" fill="currentColor"/><circle cx="8" cy="7" r="1.5" fill="currentColor"/><circle cx="19" cy="17" r="1.5" fill="currentColor"/>',
    history: '<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l3 2"/>',
    compare: '<path d="M3 6h7v12H3zM14 6h7v12h-7z"/><path d="M10 12h4M12 10v4" opacity="0.5"/>',
    eye:     '<path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7-10-7-10-7z"/><circle cx="12" cy="12" r="3"/>',
    eyeOff:  '<path d="M3 3l18 18M10.6 10.6a3 3 0 0 0 4.2 4.2M9.4 5.5A10.5 10.5 0 0 1 22 12a13 13 0 0 1-3.1 3.7M6.5 6.7A14 14 0 0 0 2 12s4 7 10 7c1.6 0 3.1-.4 4.4-1.1"/>',
    trash:   '<path d="M3 6h18M8 6V4h8v2M6 6l1 14h10l1-14"/>',
    sliders: '<line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/><circle cx="9" cy="6" r="2.2" fill="white" stroke-width="1.6"/><circle cx="15" cy="12" r="2.2" fill="white" stroke-width="1.6"/><circle cx="11" cy="18" r="2.2" fill="white" stroke-width="1.6"/>',
    settings: '<circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M5 5l2.1 2.1M16.9 16.9 19 19M5 19l2.1-2.1M16.9 7.1 19 5"/>',
    walk: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M8.155 20.999C10.985 17.274 10.631 15.784 11.339 14.592C12.047 14.89 12.401 15.635 12.931 16.082C14.347 17.423 13.462 20.403 14.7 20.85C17 21.893 15.762 17.423 15.408 16.38C15.231 15.486 14.524 15.039 13.993 14.443C12.224 11.91 12.577 13.4 13.285 9.824C14.17 10.718 13.639 10.718 14.877 11.612C17.531 13.4 17.531 12.059 17 11.463C16.47 10.867 15.408 10.718 14.7 9.377C14.17 8.185 13.462 5.801 11.693 6.397C11.339 6.546 8.685 8.632 8.332 8.93C7.801 9.526 7.801 10.271 7.624 11.165C7.27 12.506 7.093 13.102 8.508 13.102C8.862 12.357 8.862 11.91 9.039 10.867C9.393 9.377 9.393 9.973 10.278 9.228C10.101 10.42 9.216 14.89 8.862 15.933C8.332 17.572 4.793 21.148 8.155 20.999M11.339 5.056C11.516 5.503 11.162 5.652 12.047 5.95C13.462 6.248 14.524 5.056 13.993 3.715C13.285 2.374 10.455 2.97 11.339 5.056"/>',
    bike: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M17.707 17.117C17.876 16.264 16.863 15.127 16.695 13.989C20.408 13.705 21.083 18.254 18.382 19.391C15.344 20.812 12.982 16.69 15.513 14.7C16.357 15.695 16.188 17.685 17.707 17.117ZM12.475 11.999C13.994 9.441 11.969 10.294 10.619 8.872L12.644 7.735C14.332 9.583 12.306 9.441 15.513 10.862C14.163 11.289 15.007 11.004 14.501 11.857ZM11.631 15.553C10.956 14.558 11.294 13.847 11.969 12.994L14.163 12.852ZM10.112 11.004L9.944 10.578L10.112 10.72L11.463 11.146L10.619 12.284ZM10.112 14.7V15.127L9.775 14.7C9.268 13.847 9.268 14.274 9.775 13.563ZM9.606 16.406C9.1 16.832 8.256 16.832 7.749 16.548ZM8.087 17.685L7.412 17.401C8.087 17.117 8.762 17.117 9.437 17.543L9.606 17.827ZM8.087 15.695L8.762 14.842L9.268 15.553L9.437 15.979H8.931H8.256L7.918 16.264C7.749 15.553 7.749 15.979 8.087 15.695ZM9.268 18.254C8.256 20.386 5.049 20.244 4.205 17.827C3.361 15.411 5.387 13.705 7.918 14.274C7.412 15.695 6.062 16.406 6.062 16.832C6.399 18.396 7.918 17.685 9.268 18.254ZM17.876 11.146C18.214 10.862 17.538 10.72 18.382 11.004V11.573C19.057 12.284 18.551 10.436 18.551 10.009C17.538 10.009 18.214 11.289 15.176 9.583C14.332 9.156 13.825 7.024 13.657 6.029C13.319 6.171 13.488 6.171 13.488 5.745C11.463 4.75 8.425 6.74 7.749 7.877C7.412 8.872 7.918 8.446 7.243 9.583C7.074 10.151 7.243 9.014 7.074 10.151C7.412 10.578 9.944 11.857 8.931 13.136C8.087 13.705 6.399 12.426 4.543 13.847C3.192 14.842 2.686 16.406 3.192 18.112C4.374 21.381 8.593 21.523 10.281 18.538C13.994 18.112 11.8 17.401 12.644 15.837C12.644 15.695 14.838 13.421 15.007 13.279C15.007 14.416 12.306 15.269 13.825 18.538C14.669 20.386 16.863 21.239 18.72 20.386C22.095 18.964 22.095 12.71 16.188 12.994L15.513 11.573L17.37 11.715L16.188 12.426C17.876 12.568 17.707 12.426 17.876 11.146M13.488 5.745C13.825 5.745 13.825 5.603 13.657 6.029C16.019 6.74 16.863 4.892 15.682 3.755C14.669 2.618 12.813 3.613 13.488 5.745M8.931 15.979L9.268 15.553L8.762 14.842L8.087 15.695C7.918 16.122 7.918 15.979 8.256 15.979ZM9.606 16.406L7.749 16.548C8.256 16.832 9.1 16.832 9.606 16.406M9.437 17.543C8.593 17.543 8.593 17.401 8.087 17.685L9.606 17.827ZM10.112 10.72L9.944 10.578L10.112 11.004Z"/>',
    car: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M5.232 8C5.477 8.174 5.458 8.23 5.464 8.553C5.469 8.827 5.451 9.073 5.477 9.349L5.047 9.323C5.033 9.325 5.018 9.33 5.004 9.332C4.628 9.372 4.765 9.312 4.651 8.997C4.512 9.121 4.532 9.073 4.312 9.006C2.438 8.434 2.876 10.035 3.748 10.503C3.561 11.06 3.279 10.985 3.278 11.755V15.182C3.289 15.758 3.513 16.084 4.045 16.168C4.468 16.234 10.076 16.202 10.882 16.18C11.253 15.598 11.679 15.735 11.489 15.372C11.218 15.388 11.436 15.384 11.264 15.618H10.885C10.858 15.327 10.893 15.332 10.969 15.067C10.548 15.077 8.607 15.153 8.408 14.999C8.168 14.894 8.234 14.682 8.235 14.338C8.235 13.4 8.204 13.595 10.142 13.595C10.993 13.595 15.483 13.529 15.663 13.639C15.929 13.885 15.825 14.8 15.666 15.01C15.659 15.181 15.695 15.973 15.728 16.184L15.771 16.18L15.807 14.951H16.262L16.295 16.181H16.445L16.48 14.951L16.932 14.954L16.97 16.181C17.759 16.194 18.555 16.181 19.345 16.183C20.356 16.185 20.661 16.052 20.655 15.012L20.646 11.361C20.599 11.01 20.344 10.875 20.243 10.521C20.563 10.219 21.153 9.89 20.963 9.187C20.79 8.547 19.836 8.949 19.401 9.063C19.173 8.813 18.646 7.777 18.427 7.396C18.109 6.842 17.861 6.173 17.509 5.677C16.948 4.886 16.645 5.071 15.473 5.071L7.558 5.069C6.812 5.077 6.462 5.729 6.139 6.291C5.975 6.575 5.255 7.798 5.232 8M5.382 9.864L18.555 9.85L16.747 6.438C16.218 5.696 15.84 5.896 14.631 5.896C13.567 5.896 8.76 5.843 8.062 5.899C7.327 5.957 6.693 7.444 6.31 8.146C6.034 8.653 5.57 9.381 5.382 9.864M8.408 14.999L12.601 15.058L15.666 15.01C15.825 14.8 15.929 13.885 15.663 13.639C15.483 13.529 10.993 13.595 10.142 13.595C8.204 13.595 8.235 13.4 8.235 14.338C8.234 14.682 8.168 14.894 8.408 14.999M6.977 16.717C5.943 16.709 4.726 16.784 3.722 16.66C3.695 18.713 3.668 18.944 5.372 18.944C6.066 18.944 6.514 19.009 6.845 18.504C7.051 18.19 7.047 17.13 6.977 16.717M20.037 16.732L16.797 16.719C16.76 17.185 16.687 18.134 16.893 18.474C17.216 19.004 17.671 18.944 18.391 18.944C19.113 18.944 19.574 19.024 19.898 18.483C20.119 18.114 20.04 17.222 20.037 16.732M17.932 11.503C16.077 11.852 16.571 14.633 18.445 14.292C20.261 13.962 19.751 11.161 17.932 11.503M5.382 11.514C3.538 11.912 4.103 14.65 5.946 14.288C6.625 14.155 7.218 13.468 7.048 12.607C6.915 11.932 6.217 11.333 5.382 11.514M10.882 16.18C11.074 16.179 11.253 16.195 11.434 16.197C11.599 16.199 11.738 16.184 11.938 16.192L15.062 16.181C15.005 15.935 14.997 15.562 15.074 15.361H15.199C15.276 15.562 15.267 15.935 15.21 16.181L15.728 16.184C15.695 15.973 15.659 15.181 15.666 15.01L12.601 15.058L8.408 14.999C8.607 15.153 10.548 15.077 10.969 15.067C10.893 15.332 10.858 15.327 10.885 15.618H11.264C11.436 15.384 11.218 15.388 11.489 15.372C11.679 15.735 11.253 15.598 10.882 16.18M4.651 8.997C4.765 9.312 4.628 9.372 5.004 9.332C5.018 9.33 5.033 9.325 5.047 9.323L5.477 9.349C5.451 9.073 5.469 8.827 5.464 8.553C5.458 8.23 5.477 8.174 5.232 8L4.894 8.424C4.802 8.591 4.696 8.86 4.651 8.997M15.771 16.18H16.295L16.262 14.951H15.807ZM16.445 16.181H16.97L16.932 14.954L16.48 14.951ZM12.601 15.058L12.615 16.162C12.72 16.117 12.672 16.144 12.731 16.089C12.865 15.966 12.769 15.963 13.053 15.786C13.182 15.704 13.513 15.543 13.228 15.353L13.131 15.471C13.078 15.566 13.151 15.494 13.06 15.618H12.681C12.657 15.368 12.67 15.284 12.782 15.084ZM15.062 16.181H15.21C15.267 15.935 15.276 15.562 15.199 15.361H15.074C14.997 15.562 15.005 15.935 15.062 16.181"/>',
    bus: '<path fill="currentColor" stroke="none" fill-rule="evenodd" d="M5.196 7.473C4.976 7.584 4.567 7.762 4.313 7.782C3.891 7.817 3.477 7.758 3.162 7.918C2.994 8.283 2.892 10.273 3.189 10.561C3.45 10.934 4.351 10.866 4.62 10.526C4.897 9.946 4.652 8.932 4.736 8.235L5.189 8.068C5.172 10.019 5.195 11.965 5.195 13.897C5.195 15.865 4.734 17.856 6.732 18.177C6.784 19.349 6.369 20.212 7.712 20.224C9.182 20.237 8.721 19.511 8.798 18.192L15.204 18.178C15.24 19.425 14.852 20.22 16.191 20.225C17.625 20.231 17.231 19.434 17.254 18.172C19.261 17.917 18.799 15.798 18.799 13.897C18.799 12.003 18.869 9.969 18.789 8.093L19.263 8.226C19.346 8.938 19.096 9.941 19.372 10.53C19.674 10.889 20.487 10.891 20.837 10.595C21.091 10.192 21.015 8.349 20.835 7.917C20.177 7.607 19.707 7.956 18.8 7.475C18.779 3.054 18.727 3.826 11.997 3.826C10.318 3.826 8.481 3.755 6.821 3.829C4.801 3.918 5.204 6.101 5.196 7.473M8.562 6.477C6.303 6.626 6.75 6.283 6.746 10.777C6.745 11.587 6.874 11.826 7.678 11.897C10.398 12.139 12.664 12.179 15.427 11.948C17.674 11.761 17.236 12.153 17.246 7.735C17.248 6.967 17.218 6.601 16.454 6.534C13.947 6.313 11.069 6.312 8.562 6.477M9.879 16.548C10.767 16.968 13.189 16.951 14.112 16.556C14.249 16.258 14.541 14.842 14.31 14.629C14.098 14.433 9.846 14.531 9.739 14.594C9.398 14.793 9.7 16.177 9.879 16.548M8.784 5.689H15.567C15.852 5.689 16.021 5.753 16.153 5.548C16.232 5.425 16.229 5.05 16.166 4.918C16.025 4.616 15.241 4.742 14.853 4.743H8.873C8.297 4.74 7.821 4.566 7.78 5.151C7.729 5.866 8.169 5.69 8.784 5.689M7.443 14.671C6.132 15.235 6.867 17.124 8.199 16.704C9.562 16.274 8.833 14.072 7.443 14.671M15.798 14.649C14.462 15.125 15.116 17.139 16.502 16.704C17.788 16.299 17.236 14.136 15.798 14.649"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    minus: '<path d="M5 12h14"/>',
    chev: '<path d="M6 9l6 6 6-6"/>',
    close: '<path d="M18 6L6 18M6 6l12 12"/>',
    moon: '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>',
    vector: '<path d="M12 3v11"/><path d="M8 10.5 12 14.5l4-4"/><path d="M5 20h14"/>',
    recenter: '<circle cx="12" cy="12" r="3.2"/><path d="M12 2.5v3.2M12 18.3v3.2M2.5 12h3.2M18.3 12h3.2"/><circle cx="12" cy="12" r="8.2" opacity="0.45"/>',
    help: '<circle cx="12" cy="12" r="9"/><path d="M9.1 9a3 3 0 1 1 5.5 1.7c-.6.6-1.6 1-1.6 2.3M12 17v.01"/>',
    send: '<path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/>',
    // FAVE Reach Compass — accessibility rays converging on a centroid.
    // Drawn for the shell's 24×24 viewBox; explicit stroke-widths override
    // the wrapper's 1.6 default so the outer ring and inner ring read at 16 px.
    logo: '<circle cx="12" cy="12" r="9.5" stroke="currentColor" stroke-width="1.4" fill="none"/><path d="M12 3 L13 12 L12 21 L11 12 Z" fill="currentColor" stroke="none"/><path d="M3 12 L12 11 L21 12 L12 13 Z" fill="currentColor" opacity="0.55" stroke="none"/><circle cx="12" cy="12" r="1.3" fill="currentColor" stroke="none"/><circle cx="12" cy="12" r="1.3" stroke="currentColor" stroke-width="0.6" fill="none"/>',
  };
  function svg(key, size = 18) {
    return `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" style="display:block">${ICONS[key]}</svg>`;
  }

  const POI_META = [
    { id: 'grocery',           label: 'Grocery',     color: 'oklch(62% 0.14 40)' },
    { id: 'hospital',          label: 'Hospital',    color: 'oklch(58% 0.16 25)' },
    { id: 'pharmacy',          label: 'Pharmacy',    color: 'oklch(60% 0.14 340)' },
    { id: 'dentistry',         label: 'Dentistry',   color: 'oklch(60% 0.12 310)' },
    { id: 'healthcare_center', label: 'HC Center',   color: 'oklch(58% 0.13 280)' },
    { id: 'veterinary',        label: 'Veterinary',  color: 'oklch(62% 0.13 100)' },
    { id: 'university',        label: 'University',  color: 'oklch(58% 0.13 245)' },
    { id: 'kindergarten',      label: 'Kindergarten',color: 'oklch(70% 0.13 80)' },
    { id: 'school_primary',    label: 'Primary.S',   color: 'oklch(62% 0.12 200)' },
    { id: 'school_high',       label: 'High.S',      color: 'oklch(55% 0.13 220)' },
  ];
  const CITIES = [
    { key: 'vaxjo',      label: 'Växjö' },
    { key: 'goteborg',   label: 'Göteborg' },
    { key: 'malmo',      label: 'Malmö' },
    { key: 'stockholm',  label: 'Stockholm' },
    { key: 'kalmar',     label: 'Kalmar' },
    { key: 'norrkoping', label: 'Norrköping' },
    { key: 'uppsala',    label: 'Uppsala' },
  ];

  // ---------------- topbar ----------------
  function buildTopbar() {
    const top = document.createElement('header');
    top.className = 'fave-topbar';
    top.id = 'faveTopbar';
    top.innerHTML = `
      <div class="topbar-section brand">
        <div class="brand-mark" data-tooltip="FAVE — Fairness Accessibility Visual Explorer" data-tooltip-pos="below">${svg('logo', 16)}</div>
        <div class="brand-name"><strong>FAVE</strong></div>
      </div>
      <div class="topbar-divider"></div>
      <div class="topbar-section">
        <div class="field">
          <span class="field-label">City</span>
          <button class="control" id="topCityBtn" type="button" data-tooltip="Choose city" data-tooltip-pos="below" aria-label="Choose city">
            <span id="topCityLabel">Växjö</span>${svg('chev', 11)}
          </button>
        </div>
        <span id="topJobStatus" style="font-size:11px;color:var(--ink-3);"></span>
      </div>
      <div class="topbar-divider"></div>
      <div class="topbar-section">
        <div class="field">
          <span class="field-label">Model</span>
          <div class="segmented">
            <button data-active="true" data-model="gravity" type="button" data-tooltip="Gravity-based fairness" data-tooltip-pos="below" aria-label="Gravity model">Gravity</button>
            <button data-active="false" data-model="distance" type="button" data-tooltip="Distance-based fairness" data-tooltip-pos="below" aria-label="Distance model">Distance</button>
          </div>
        </div>
      </div>
      <div class="topbar-spacer"></div>
      <div class="topbar-section">
        <button class="icon-btn" id="topExportBtn" data-tooltip="Export interface as vector SVG (opens in CorelDraw)" data-tooltip-pos="below" aria-label="Export interface as vector SVG" type="button">${svg('vector', 14)}</button>
        <button class="icon-btn" id="topResetViewBtn" data-tooltip="Reset map view — re-frame the whole city" data-tooltip-pos="below" aria-label="Reset map view" type="button">${svg('recenter', 14)}</button>
        <button class="icon-btn" id="topThemeBtn" data-tooltip="Toggle light/dark theme" data-tooltip-pos="below" aria-label="Toggle theme" type="button">${svg('moon', 14)}</button>
        <button class="icon-btn" id="topHelpBtn" data-tooltip="Help · FAVE guide" data-tooltip-pos="below" aria-label="Help" type="button">${svg('help', 14)}</button>
      </div>
    `;
    return top;
  }

  // ---------------- rail ----------------
  function buildRail() {
    const rail = document.createElement('aside');
    rail.className = 'fave-rail';
    rail.id = 'faveRail';
    const sections = [
      { label: 'SCALE', items: [
        { id: 'rail-macro', icon: 'district', tip: 'Macro · Districts',  click: () => clickIfExists('districtToggleBtn') },
        { id: 'rail-meso',  icon: 'hexagons', tip: 'Meso · Hexagons',     click: () => toggleMesoPopover() },
        { id: 'rail-micro', icon: 'building', tip: 'Micro · Buildings',   click: () => clickIfExists('microToggleBtn') },
      ]},
      { label: 'TOOLS', items: [
        { id: 'rail-poi',    icon: 'layers',  tip: 'POIs & weights',     click: () => togglePOIPopover() },
        { id: 'rail-equity', icon: 'sparkle', tip: 'Equity analysis',    click: () => toggleEquityAnalysis() },
        { id: 'rail-wif',    icon: 'wand',    tip: 'What-if scenario',   click: () => toggleWhatIfPopover() },
        { id: 'rail-changes',icon: 'compare', tip: 'Changes',            click: () => toggleChangesPopover() },
      ]},
      { label: 'VIEWS', items: [
        { id: 'rail-parallel', icon: 'parallel', tip: 'Parallel coords', click: () => openParallelDrawer() },
        { id: 'rail-dr',       icon: 'scatter',  tip: 'DR Explorer',     click: () => openDRDrawer() },
      ]},
    ];
    const sides = [
      { id: 'rail-ai',        icon: 'sparkle',  tip: 'Ask the map',      click: () => toggleAISidebar() },
      { id: 'rail-history',   icon: 'history',  tip: 'History stack',    click: () => toggleHistoryPopover() },
      { id: 'rail-inspector', icon: 'sliders',  tip: 'Toggle inspector', click: () => toggleInspector() },
    ];
    function btn(item) {
      const b = document.createElement('button');
      b.className = 'rail-btn';
      b.id = item.id;
      b.type = 'button';
      b.setAttribute('data-tooltip', item.tip);
      b.setAttribute('data-tooltip-pos', 'right');
      b.setAttribute('aria-label', item.tip);
      b.innerHTML = svg(item.icon, 18);
      b.addEventListener('click', item.click);
      return b;
    }
    for (const sec of sections) {
      const lbl = document.createElement('div');
      lbl.className = 'rail-section-label';
      lbl.textContent = sec.label;
      rail.appendChild(lbl);
      for (const it of sec.items) rail.appendChild(btn(it));
      const div = document.createElement('div');
      div.className = 'rail-divider';
      rail.appendChild(div);
    }
    for (const it of sides) rail.appendChild(btn(it));
    const spacer = document.createElement('div');
    spacer.style.flex = '1';
    rail.appendChild(spacer);
    return rail;
  }

  // ---------------- map overlays ----------------
  function buildLegend() {
    const w = document.createElement('div');
    w.className = 'shell-legend';
    // Tabs choose the map color VARIABLE (Fairness vs 2SFCA Supply provision);
    // both share the ramp below so the two views are directly comparable. The
    // scale slot is filled by the experimental absolute-scale module
    // (lib/absoluteColorScale.js) and stays empty/hidden when it's removed.
    // Each tab carries a small "?" that toggles a plain-English explanation of
    // that metric (text + behavior live in lib/supplyProvisionLens.js).
    const help = '<svg viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M9.1 9a3 3 0 1 1 5.5 1.7c-.6.6-1.6 1-1.6 2.3M12 17v.01"/></svg>';
    const tab = (v, label, active) =>
      `<span class="legend-tab-group">` +
      `<button class="legend-tab" type="button" role="tab" data-colorvar="${v}" data-active="${active}">${label}</button>` +
      `<button class="legend-tab-help" type="button" data-help="${v}" aria-label="About ${label}" aria-expanded="false">${help}</button>` +
      `</span>`;
    w.innerHTML = `
      <div class="legend-tabs" role="tablist" aria-label="Map color variable">
        ${tab('fairness', 'Fairness', 'true')}
        ${tab('supply', 'Supply', 'false')}
        ${tab('mismatch', 'Mismatch', 'false')}
        ${tab('priority', 'Priority', 'false')}
      </div>
      <div class="legend-priority-need" id="legendPriorityNeed" hidden>
        <label for="priorityNeedSel">Need</label>
        <select id="priorityNeedSel" class="form-select form-select-sm">
          <option value="needZ">Deprivation</option>
          <option value="child_frac">Children</option>
          <option value="elder_frac">Elderly</option>
          <option value="income">Low income</option>
          <option value="higher_ed">Low education</option>
        </select>
      </div>
      <div class="ramp"></div>
      <div class="ramp-labels"><span>Least fair</span><span>Medium</span><span>Most fair</span></div>
      <div class="legend-scale" id="legendScaleSlot"></div>
      <div class="legend-sub" id="legendNote">Building color = accessibility fairness (greener = fairer).</div>
      <div class="legend-help" id="legendHelp" hidden></div>
    `;
    return w;
  }
  function buildZoom() {
    const w = document.createElement('div');
    w.className = 'shell-zoom';
    w.id = 'shellZoom';
    w.innerHTML = `
      <button id="shellZoomIn" data-tooltip="Zoom in" aria-label="Zoom in" type="button">${svg('plus', 14)}</button>
      <button id="shellZoomOut" data-tooltip="Zoom out" aria-label="Zoom out" type="button">${svg('minus', 14)}</button>
    `;
    return w;
  }
  function buildTravel() {
    const w = document.createElement('div');
    w.className = 'shell-travel';
    w.id = 'shellTravel';
    const modes = [
      { id: 'walking', icon: 'walk', label: 'Walk', tip: 'Walking distance' },
      { id: 'cycling', icon: 'bike', label: 'Cycle', tip: 'Cycling distance' },
      { id: 'driving', icon: 'car',  label: 'Driving', tip: 'Driving distance' },
      { id: 'transit', icon: 'bus',  label: 'Transit', tip: 'Public transport' },
    ];
    w.innerHTML = modes.map(m =>
      `<button data-mode="${m.id}" data-active="${m.id === 'walking' ? 'true' : 'false'}" type="button" data-tooltip="${m.tip}" aria-label="${m.tip}">${svg(m.icon, 13)} ${m.label}</button>`
    ).join('');
    return w;
  }

  // ---------------- popovers ----------------
  function buildCityMenu() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-city-menu';
    p.id = 'shellCityMenu';
    p.innerHTML = `
      <div class="popover-body">
        ${CITIES.map(c => `<button class="city-row" data-key="${c.key}" type="button">${c.label}</button>`).join('')}
      </div>
    `;
    return p;
  }

  function buildPOIPopover() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-poi-menu';
    p.id = 'shellPOIPopover';
    // Use the project's legend-*.svg files for each POI category. They
    // sit at frontend/assets/icons and are named legend-<id>.svg.
    const rows = POI_META.map(m =>
      `<div class="poi-row" data-cat="${m.id}" data-checked="false">
        <label>
          <input type="checkbox" class="shell-poi-check" data-cat="${m.id}">
          <span class="pl-icon">
            <img src="assets/icons/legend-${m.id}.svg" alt="" aria-hidden="true" class="poi-legend-img">
          </span>
          <span>${m.label}</span>
        </label>
        <div></div>
        <input type="range" min="1" max="10" step="1" value="5" class="shell-poi-weight" data-cat="${m.id}" disabled>
        <span class="num shell-poi-numb" data-cat="${m.id}">5</span>
      </div>`
    ).join('');
    p.innerHTML = `
      <div class="popover-header">
        <span class="title">${svg('layers', 13)} POIs &amp; weights</span>
        <button class="close-btn" data-close="poi" type="button">${svg('close', 14)}</button>
      </div>
      <div class="popover-body">
        <div style="font-size:11px;color:var(--ink-3);line-height:1.5;margin-bottom:10px;">
          Select categories and weights (1–10). Map updates live.
        </div>
        ${rows}
        <div class="poi-foot-row">
          <button class="shell-btn" id="shellPOIClear" type="button">Clear</button>
          <button class="shell-btn" id="shellPOIAll" type="button">All</button>
          <span class="spacer"></span>
          <label class="toggle"><input type="checkbox" id="shellPOISymbolsToggle" checked> Symbols</label>
        </div>
      </div>
    `;
    return p;
  }

  function buildChangesPopover() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-changes';
    p.id = 'shellChanges';
    // Explicitly closed at boot. The popover only opens when the user
    // clicks the rail's Changes button (toggleChangesPopover).
    p.setAttribute('data-open', 'false');
    p.innerHTML = `
      <div class="popover-header">
        <span class="title">${svg('compare', 13)} Changes <span class="changes-meta tiny muted"></span></span>
        <button class="close-btn" data-close="changes" type="button" title="Close" aria-label="Close">${svg('close', 14)}</button>
      </div>
      <div class="popover-body" id="shellChangesBody">
        <div class="changes-empty">No changes recorded yet.</div>
      </div>
      <div class="popover-footer">
        <button class="shell-btn" id="shellChangesCompare" type="button">${svg('sparkle', 11)} Compare baseline</button>
        <button class="shell-btn shell-btn-danger" id="shellChangesClear" type="button">${svg('trash', 11)} Clear all changes</button>
      </div>
    `;
    return p;
  }

  function buildHistoryPopover() {
    const p = document.createElement('div');
    p.className = 'shell-popover shell-history';
    p.id = 'shellHistory';
    p.innerHTML = `
      <div class="popover-header">
        <span class="title">${svg('history', 13)} History stack</span>
        <span style="display:flex;gap:6px;align-items:center;">
          <button class="shell-btn" id="shellHistoryClear" type="button" title="Clear all">Clear all</button>
          <button class="close-btn" data-close="history" type="button" title="Close" aria-label="Close">${svg('close', 14)}</button>
        </span>
      </div>
      <div class="popover-body" id="shellHistoryBody">
        <div class="history-empty">No changes yet.</div>
      </div>
    `;
    return p;
  }

  function buildAISidebar() {
    const a = document.createElement('aside');
    a.className = 'shell-ai';
    a.id = 'shellAI';
    a.innerHTML = `
      <div class="ai-header">
        <div class="title"><span class="glow"></span> Ask the map</div>
        <button class="close-btn" data-close="ai" type="button" style="background:transparent;border:0;color:var(--ink-3);cursor:pointer;width:24px;height:24px;border-radius:4px;">${svg('close', 14)}</button>
      </div>
      <div class="ai-body" id="shellAIBody">
        <div class="ai-msg">
          I can load cities, change modes, compute fairness, or explain what you see on the map.
          Try one of these:
        </div>
        <div class="ai-suggest" id="shellAISuggest">
          <button type="button" data-prompt="Fairness for hospitals on cycling distance">Fairness for hospitals on cycling distance</button>
          <button type="button" data-prompt="Fairness for veterinary with weight 1 and university with weight 10">Fairness for veterinary w=1, university w=10</button>
          <button type="button" data-prompt="Show fairness for all">Show fairness for all POIs</button>
          <button type="button" data-prompt="Show districts mode and the statistic for Araby">Districts mode · stats for Araby</button>
          <button type="button" data-prompt="Switch to walking distance and recompute">Switch to walking distance</button>
          <button type="button" data-prompt="Where should I add 5 grocery stores to improve fairness?">Where to add 5 grocery stores?</button>
        </div>
      </div>
      <div class="ai-input-wrap">
        <textarea class="ai-input" id="shellAIInput" placeholder="Ask anything about fairness, scale, modes, or sites…"></textarea>
        <button class="shell-btn shell-btn-primary" id="shellAISend" type="button">${svg('send', 12)} Send</button>
      </div>
    `;
    return a;
  }

  // ---------------- JS-positioned tooltip ----------------
  let _tip = null;
  function getTip() {
    if (_tip) return _tip;
    _tip = document.createElement('div');
    _tip.className = 'fave-tip';
    _tip.setAttribute('aria-hidden', 'true');
    _tip.setAttribute('data-export-ignore', '1'); // transient hover chrome, never exported
    document.body.appendChild(_tip);
    return _tip;
  }
  function showTip(el) {
    const text = el.getAttribute('data-tooltip');
    if (!text) return;
    const t = getTip();
    t.textContent = text;
    // Force a layout pass so we can read its size before positioning.
    t.style.left = '0px';
    t.style.top = '0px';
    t.setAttribute('data-shown', 'true');
    const tipRect = t.getBoundingClientRect();
    const r = el.getBoundingClientRect();
    const pos = el.getAttribute('data-tooltip-pos') || 'right';
    let left, top;
    if (pos === 'below') {
      left = r.left + r.width / 2 - tipRect.width / 2;
      top = r.bottom + 6;
    } else if (pos === 'above') {
      left = r.left + r.width / 2 - tipRect.width / 2;
      top = r.top - tipRect.height - 6;
    } else if (pos === 'left') {
      left = r.left - tipRect.width - 8;
      top = r.top + r.height / 2 - tipRect.height / 2;
    } else { // right (default — used for rail buttons)
      left = r.right + 8;
      top = r.top + r.height / 2 - tipRect.height / 2;
    }
    // Keep on-screen
    left = Math.max(4, Math.min(left, window.innerWidth - tipRect.width - 4));
    top = Math.max(4, Math.min(top, window.innerHeight - tipRect.height - 4));
    t.style.left = `${left}px`;
    t.style.top = `${top}px`;
  }
  function hideTip() {
    if (_tip) _tip.setAttribute('data-shown', 'false');
  }
  function wireTooltipEvents() {
    document.addEventListener('mouseover', (e) => {
      const el = e.target.closest && e.target.closest('[data-tooltip]');
      if (el) showTip(el);
    });
    document.addEventListener('mouseout', (e) => {
      const el = e.target.closest && e.target.closest('[data-tooltip]');
      if (el) hideTip();
    });
    document.addEventListener('focusin', (e) => {
      if (e.target.matches && e.target.matches('[data-tooltip]')) showTip(e.target);
    });
    document.addEventListener('focusout', (e) => {
      if (e.target.matches && e.target.matches('[data-tooltip]')) hideTip();
    });
  }

  // ---------------- helpers ----------------
  function clickIfExists(id) {
    const el = document.getElementById(id);
    if (el) el.click();
  }
  function clickAtSelector(sel) {
    const el = document.querySelector(sel);
    if (el) el.click();
  }
  function isDROpen() {
    const el = document.getElementById('drOffcanvas');
    return el && el.classList.contains('show');
  }
  function isParallelOpen() {
    const el = document.getElementById('parallelCoordsPanel');
    return el && !el.classList.contains('d-none');
  }
  function closeDR() {
    const el = document.getElementById('drOffcanvas');
    if (!el || typeof bootstrap === 'undefined') return;
    if (isDROpen()) bootstrap.Offcanvas.getOrCreateInstance(el).hide();
  }
  function closeParallel() {
    if (isParallelOpen()) clickIfExists('parallelCoordsBtn');  // legacy toggle
  }
  function openDRDrawer() {
    // Allow Parallel and DR to coexist as side-by-side panes in the bottom
    // drawer (matches the design's AnalyticsDrawer). Old behavior closed
    // Parallel before opening DR; new behavior leaves both visible.
    const el = document.getElementById('drOffcanvas');
    if (!el || typeof bootstrap === 'undefined') return clickIfExists('openDRBtn');
    bootstrap.Offcanvas.getOrCreateInstance(el).toggle();
  }
  function openParallelDrawer() {
    clickIfExists('parallelCoordsBtn');
  }
  function toggleInspector() {
    const insp = document.getElementById('inspector');
    if (!insp) return;
    const open = insp.getAttribute('data-open') === 'true';
    // Inspector and Ask-the-map are mutually exclusive: both occupy the
    // right side of the screen, so opening one closes the other.
    if (!open) {
      const ai = document.getElementById('shellAI');
      if (ai && ai.getAttribute('data-open') === 'true') toggleAISidebar();
    }
    insp.setAttribute('data-open', open ? 'false' : 'true');
    syncInspectorBody();
  }
  function syncInspectorBody() {
    const insp = document.getElementById('inspector');
    if (!insp) return;
    const open = insp.getAttribute('data-open') === 'true';
    document.body.setAttribute('data-inspector-open', open ? 'true' : 'false');
    const railBtn = document.getElementById('rail-inspector');
    if (railBtn) railBtn.setAttribute('data-active', open ? 'true' : 'false');
    scheduleMapResize();
  }

  // Tell MapLibre + deck.gl that the canvas size changed. The CSS transition
  // for the inspector/AI sidebar is ~250 ms so we resize once at frame 1
  // (so deck rebuilds its canvas) and again after the transition settles.
  let _resizeRAF = null;
  function doResize() {
    try { if (typeof map !== 'undefined' && map?.resize) map.resize(); } catch (_) {}
    try { window.dispatchEvent(new Event('resize')); } catch (_) {}
    try { if (typeof overlay !== 'undefined' && overlay?.deck?.redraw) overlay.deck.redraw(true); } catch (_) {}
    try { if (typeof updateLayers === 'function') updateLayers(); } catch (_) {}
  }
  function scheduleMapResize() {
    if (_resizeRAF) cancelAnimationFrame(_resizeRAF);
    _resizeRAF = requestAnimationFrame(() => {
      _resizeRAF = null;
      doResize();
      // Run again at the start, middle, and end of the inspector/AI slide
      // transition (~250 ms) so the deck.gl canvas always tracks the live
      // container size — single resize wasn't enough on Firefox.
      setTimeout(doResize, 100);
      setTimeout(doResize, 360);
      // (Previously fitToData(baseCityFC) ran here so the city's right
      // edge wasn't clipped under the inspector. Removed: the user wants
      // the camera to stay where they put it across inspector toggles
      // and hover-driven inspector updates.)
    });
  }

  function syncScaleActiveStates() {
    const macro = document.getElementById('rail-macro');
    const meso = document.getElementById('rail-meso');
    const micro = document.getElementById('rail-micro');
    const inDistrict = (typeof districtView !== 'undefined' && districtView);
    const inMezo = (typeof mezoView !== 'undefined' && mezoView);
    if (macro) macro.setAttribute('data-active', inDistrict && !inMezo ? 'true' : 'false');
    if (meso)  meso.setAttribute('data-active',  inMezo ? 'true' : 'false');
    if (micro) micro.setAttribute('data-active', !inDistrict && !inMezo ? 'true' : 'false');
  }

  // Reflect "POIs & weights has a non-empty selection" on the rail-poi
  // button so it lights up orange like the SCALE buttons. Reads the legacy
  // .poi-check checkboxes (the source of truth that drives selectedPOIMix).
  // Called from change handlers, from closeAllPopovers, and exposed on
  // window so history/state restores can sync after they programmatically
  // toggle checkboxes (which doesn't fire 'change').
  function syncPOIRailActive() {
    const rail = document.getElementById('rail-poi');
    if (!rail) return;
    const anyChecked = document.querySelectorAll('.poi-check:checked').length > 0;
    rail.setAttribute('data-active', anyChecked ? 'true' : 'false');
  }
  window.syncPOIRailActive = syncPOIRailActive;

  // ---------------- popover toggling ----------------
  function closeEquityPanel() {
    const panel = document.getElementById('equityAnalysisPanel');
    if (!panel || panel.classList.contains('d-none')) return;
    panel.classList.add('d-none');
    const rail = document.getElementById('rail-equity');
    if (rail) rail.setAttribute('data-active', 'false');
  }

  function closeAllPopovers(except) {
    document.querySelectorAll('.shell-popover[data-open="true"]').forEach(p => {
      if (p !== except) {
        p.setAttribute('data-open', 'false');
        // Reset the corresponding rail button's active state so the accent
        // stripe disappears when the panel closes. EXCEPT rail-poi, whose
        // active state tracks "non-empty POI selection", not "popover open"
        // — that's resynced via syncPOIRailActive() below.
        const railIdByPanelId = {
          shellChanges: 'rail-changes',
          shellHistory: 'rail-history',
          shellWhatIf:  'rail-wif',
        };
        const railBtnId = railIdByPanelId[p.id];
        if (railBtnId) {
          const rail = document.getElementById(railBtnId);
          if (rail) rail.setAttribute('data-active', 'false');
        }
      }
    });
    // Equity analysis is a legacy panel (not a .shell-popover), so it
    // wasn't covered by the loop above. Close it too so the TOOLS group
    // (POIs / Equity / What-if / Changes) stays mutually exclusive.
    closeEquityPanel();
    syncPOIRailActive();
  }
  function positionPopoverNear(popover, anchor, opts = {}) {
    const r = anchor.getBoundingClientRect();
    if (opts.right) {
      // For rail buttons (anchored to the right of the rail).
      popover.style.left = `${r.right + 10}px`;
      popover.style.top = `${Math.max(60, r.top)}px`;
    } else {
      // For topbar buttons (anchored below).
      popover.style.left = `${r.left}px`;
      popover.style.top = `${r.bottom + 6}px`;
    }
  }
  function togglePOIPopover() {
    const pop = document.getElementById('shellPOIPopover');
    const anchor = document.getElementById('rail-poi');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      positionPopoverNear(pop, anchor, { right: true });
      pop.setAttribute('data-open', 'true');
      syncPOIPopoverFromLegacy();
    }
  }
  function toggleCityMenu() {
    const pop = document.getElementById('shellCityMenu');
    const anchor = document.getElementById('topCityBtn');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      positionPopoverNear(pop, anchor);
      pop.setAttribute('data-open', 'true');
      syncCityMenu();
    }
  }
  // Build a What-if popover by lifting the legacy .whatif-menu out of the
  // hidden navbar's Bootstrap dropdown and wrapping it in a free-standing
  // .shell-popover. The legacy form controls keep their IDs so all the
  // existing what-if JS handlers continue to fire.
  function ensureWhatIfPopover() {
    const existing = document.getElementById('shellWhatIf');
    if (existing) return existing;
    const menu = document.querySelector('.whatif-menu');
    if (!menu) return null;
    const wrap = document.createElement('div');
    wrap.className = 'shell-popover';
    wrap.id = 'shellWhatIf';
    wrap.style.width = '420px';
    wrap.style.maxHeight = 'calc(100vh - 100px)';
    const header = document.createElement('div');
    header.className = 'popover-header';
    header.innerHTML = `<span class="title">${svg('wand', 13)} What-if scenario</span>
      <button class="close-btn" data-close="wif" type="button" aria-label="Close">${svg('close', 14)}</button>`;
    wrap.appendChild(header);
    const body = document.createElement('div');
    body.className = 'popover-body';
    // Detach the legacy menu from its <div class="dropdown"> parent and stash
    // it inside the popover body. All event listeners on the form controls
    // travel with the elements.
    body.appendChild(menu);
    // Strip Bootstrap classes that would keep it hidden as a dropdown.
    menu.classList.remove('dropdown-menu', 'dropdown-menu-end', 'show');
    menu.style.position = 'static';
    menu.style.padding = '0';
    menu.style.boxShadow = 'none';
    menu.style.border = '0';
    menu.style.display = 'block';
    menu.style.width = '100%';
    menu.style.maxWidth = '100%';
    menu.style.minWidth = '0';
    menu.style.overflowX = 'hidden';
    body.style.overflowY = 'auto';
    body.style.maxHeight = 'calc(100vh - 160px)';
    wrap.appendChild(body);
    document.body.appendChild(wrap);
    wrap.querySelector('[data-close="wif"]').addEventListener('click', () => {
      wrap.setAttribute('data-open', 'false');
      const railBtn = document.getElementById('rail-wif');
      if (railBtn) railBtn.setAttribute('data-active', 'false');
    });
    return wrap;
  }

  function toggleWhatIfPopover() {
    const pop = ensureWhatIfPopover();
    const anchor = document.getElementById('rail-wif');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      positionPopoverNear(pop, anchor, { right: true });
      // The What-if form is tall (mode picker + new-POI + mock-building
      // controls). Pin it near the top so the bottom doesn't fall off the
      // viewport from a mid-rail anchor — the user shouldn't need to scroll
      // the popover to reach the action buttons.
      pop.style.top = '60px';
      pop.setAttribute('data-open', 'true');
    }
    const railBtn = document.getElementById('rail-wif');
    if (railBtn) railBtn.setAttribute('data-active', isOpen ? 'false' : 'true');
  }

  // Meso popover — built once on demand. Lists the same edge-size options
  // as the legacy navbar dropdown (`.mezo-edge-option`) plus a "Hide meso"
  // row, so the rail button covers both the toggle and the size selection.
  function ensureMesoPopover() {
    const existing = document.getElementById('shellMeso');
    if (existing) return existing;
    const wrap = document.createElement('div');
    wrap.className = 'shell-popover';
    wrap.id = 'shellMeso';
    wrap.style.width = '200px';
    const header = document.createElement('div');
    header.className = 'popover-header';
    header.innerHTML = `<span class="title">${svg('hexagons', 13)} Meso · Hex size</span>
      <button class="close-btn" data-close="meso" type="button" aria-label="Close">${svg('close', 14)}</button>`;
    wrap.appendChild(header);
    const body = document.createElement('div');
    body.className = 'popover-body';
    // H3 only exposes a few discrete edge lengths in the city-scale band:
    // res 7 ≈ 1.22 km, res 8 ≈ 0.46 km, res 9 ≈ 0.17 km. 0.5 km and 0.75 km
    // both round to res 8 and rendered identically — drop 0.75 from the
    // picker so the three options map to three distinct hex sizes.
    const sizes = [
      { value: '0.25', label: '0.25 km' },
      { value: '0.5',  label: '0.5 km'  },
      { value: '1',    label: '1 km'    },
    ];
    body.innerHTML = `
      <div class="meso-size-list">
        ${sizes.map(s => `<button type="button" class="meso-size-row" data-value="${s.value}">${s.label}</button>`).join('')}
      </div>
    `;
    wrap.appendChild(body);
    document.body.appendChild(wrap);

    // Highlight a size only once meso is actually on. Before the user picks
    // anything we leave every row neutral so the default value (0.25 km)
    // doesn't masquerade as "user's choice".
    const refreshActive = () => {
      const isMezoOn = (typeof mezoView !== 'undefined' && mezoView);
      const cur = isMezoOn && typeof selectedMezoHexEdgeKm !== 'undefined'
        ? String(selectedMezoHexEdgeKm) : '';
      wrap.querySelectorAll('.meso-size-row').forEach(b => {
        b.setAttribute('data-active', cur && String(b.dataset.value) === cur ? 'true' : 'false');
      });
    };
    refreshActive();

    wrap.querySelectorAll('.meso-size-row').forEach(btn => {
      btn.addEventListener('click', () => {
        const val = btn.dataset.value;
        // Drive the legacy `.mezo-edge-option` so the existing handler runs
        // (setMezoHexEdgeKm + setMezoView(true) + recompute) — single source
        // of truth for the meso size pipeline.
        const legacy = document.querySelector(`.mezo-edge-option[data-value="${val}"]`);
        if (legacy) legacy.click();
        refreshActive();
      });
    });
    wrap.querySelector('[data-close="meso"]').addEventListener('click', () => {
      wrap.setAttribute('data-open', 'false');
    });
    // Expose the refresher so toggleMesoPopover can re-sync without
    // duplicating the highlight logic.
    wrap.__refreshActive = refreshActive;
    return wrap;
  }

  function toggleMesoPopover() {
    const pop = ensureMesoPopover();
    const anchor = document.getElementById('rail-meso');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      // First time selecting meso: default to 0.25 km and apply it to the
      // map immediately, so the user sees hexagons without a second click.
      // Delegates to the popover's own row click → legacy .mezo-edge-option
      // → setMezoHexEdgeKm + setMezoView(true) → refreshMezoScores +
      // updateLayers. If meso is already on we leave the current size alone.
      if (typeof mezoView !== 'undefined' && !mezoView) {
        const defaultRow = pop.querySelector('.meso-size-row[data-value="0.25"]');
        if (defaultRow) defaultRow.click();
      }
      pop.__refreshActive?.();
      positionPopoverNear(pop, anchor, { right: true });
      pop.setAttribute('data-open', 'true');
    }
  }

  // Equity Analysis lives in the legacy panel (#equityAnalysisPanel toggled
  // via inline onclick). Forward the click to that handler, then mirror the
  // panel's visibility back onto the rail button so the orange accent treats
  // "panel open" as active state. Opening it closes all popovers so the
  // TOOLS group stays mutually exclusive (Equity / POIs / What-if /
  // Changes can't be on at the same time).
  function toggleEquityAnalysis() {
    const panel = document.getElementById('equityAnalysisPanel');
    if (!panel) return;
    const willOpen = panel.classList.contains('d-none');
    if (willOpen) closeAllPopovers();
    panel.classList.toggle('d-none');
    const rail = document.getElementById('rail-equity');
    if (rail) {
      rail.setAttribute('data-active', panel.classList.contains('d-none') ? 'false' : 'true');
    }
  }

  function ensureHelpModal() {
    const existing = document.getElementById('shellHelpModal');
    if (existing) return existing;
    const wrap = document.createElement('div');
    wrap.id = 'shellHelpModal';
    wrap.className = 'shell-help-modal';
    wrap.setAttribute('data-open', 'false');
    wrap.innerHTML = `
      <div class="shell-help-backdrop" data-close-help></div>
      <div class="shell-help-card" role="dialog" aria-modal="true" aria-labelledby="shellHelpTitle">
        <div class="shell-help-head">
          <div class="shell-help-brand">
            <span class="brand-mark" aria-hidden="true">${svg('logo', 16)}</span>
            <div>
              <div class="shell-help-title" id="shellHelpTitle">FAVE — Fairness, Accessibility &amp; Visualization Explorer</div>
              <div class="shell-help-sub">Quick guide to the interface</div>
            </div>
          </div>
          <button class="close-btn" type="button" data-close-help aria-label="Close">${svg('close', 14)}</button>
        </div>
        <div class="shell-help-body">
          <section class="help-callout">
            <div class="help-callout-icon">${svg('logo', 22)}</div>
            <div>
              <strong>FAVE</strong> measures and visualises how fairly a city distributes access to amenities (POIs). Every building gets a 0–1 fairness score per category and a combined overall score; the map colours buildings, hex cells, or districts on that ramp.
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('sparkle', 13)}</span> Top metric strip</h4>
            <div class="help-grid">
              <div class="help-card">
                <div class="help-card-head"><span class="dir-up">↑</span> Overall</div>
                <p>Mean per-building fairness (0–1). Higher = more fair on average.</p>
              </div>
              <div class="help-card">
                <div class="help-card-head"><span class="dir-down">↓</span> Gini</div>
                <p>Inequality of fairness across the city. Lower = more evenly distributed.</p>
              </div>
              <div class="help-card">
                <div class="help-card-head">POIs</div>
                <p>Total points-of-interest in the active categories.</p>
              </div>
              <div class="help-card">
                <div class="help-card-head">Districts · Cells · Buildings</div>
                <p>Count of units at the active scale (Macro / Meso / Micro).</p>
              </div>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('hexagons', 13)}</span> Scale · left rail</h4>
            <div class="help-grid help-grid-3">
              <div class="help-card help-card-row">
                <span class="help-icon-tile">${svg('district', 14)}</span>
                <div><strong>Macro</strong><p>RegSO districts.</p></div>
              </div>
              <div class="help-card help-card-row">
                <span class="help-icon-tile">${svg('hexagons', 14)}</span>
                <div><strong>Meso</strong><p>Hex grid. Pick edge size 0.25 / 0.5 / 0.75 / 1 km.</p></div>
              </div>
              <div class="help-card help-card-row">
                <span class="help-icon-tile">${svg('building', 14)}</span>
                <div><strong>Micro</strong><p>Individual buildings (default).</p></div>
              </div>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('layers', 13)}</span> POIs &amp; Weights</h4>
            <p>Open the layers popover, tick the categories you care about and weight each 1–10. The map recolours immediately. Flip <em>Symbols</em> to show or hide POI pins.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('walk', 13)}</span> Travel mode · bottom-centre</h4>
            <p>Drives the time → fairness conversion. The selected mode shows the orange-accent box.</p>
            <div class="help-mode-row">
              <span class="help-mode help-mode-active">${svg('walk', 13)} Walk</span>
              <span class="help-mode">${svg('bike', 13)} Cycle</span>
              <span class="help-mode">${svg('car', 13)} Driving</span>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('compare', 13)}</span> Model · top bar</h4>
            <div class="help-grid">
              <div class="help-card"><div class="help-card-head">Gravity</div><p>IF-City utility model — closer POIs and crowding both matter.</p></div>
              <div class="help-card"><div class="help-card-head">Distance</div><p>Straight nearest-POI travel time.</p></div>
            </div>
          </section>

          <section>
            <h4><span class="help-icon">${svg('wand', 13)}</span> What-if &amp; Changes</h4>
            <p>Add or change buildings to test scenarios. The Changes panel logs each tweak so you can compare before-vs-after Gini and overall fairness.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('sliders', 13)}</span> Inspector · right</h4>
            <p>Click a building, hex, or district to see its score, per-category breakdown, and population/income context. Toggle from the rail's slider icon.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('scatter', 13)}</span> DR Explorer &amp; Parallel Coords</h4>
            <p>Bottom drawers for projection-based exploration. Lasso a cluster on the DR scatter to drive the EBM explanation, or drag PC axes to filter buildings by raw features.</p>
          </section>

          <section>
            <h4><span class="help-icon">${svg('sparkle', 13)}</span> Ask the map · AI</h4>
            <p>Open the AI panel and type a natural-language request. Try one of these:</p>
            <ul class="help-examples">
              <li><code>Fairness for hospitals on cycling distance</code></li>
              <li><code>Fairness for veterinary with weight 1 and university with weight 10</code></li>
              <li><code>Show fairness for all</code></li>
              <li><code>Show districts mode and the statistic for Araby</code></li>
              <li><code>Where should I add 5 grocery stores to improve fairness?</code></li>
            </ul>
          </section>

          <section class="shell-help-foot">
            <div>FAVE is research software developed at Linnaeus University.</div>
            <div>Tile data © OpenStreetMap contributors · Esri.</div>
          </section>
        </div>
      </div>
    `;
    document.body.appendChild(wrap);
    wrap.querySelectorAll('[data-close-help]').forEach(el => {
      el.addEventListener('click', () => wrap.setAttribute('data-open', 'false'));
    });
    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && wrap.getAttribute('data-open') === 'true') {
        wrap.setAttribute('data-open', 'false');
      }
    });
    return wrap;
  }

  function toggleHelpModal() {
    const m = ensureHelpModal();
    const open = m.getAttribute('data-open') === 'true';
    m.setAttribute('data-open', open ? 'false' : 'true');
  }

  function toggleChangesPopover() {
    const pop = document.getElementById('shellChanges');
    const anchor = document.getElementById('rail-changes');
    if (!pop || !anchor) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      // Anchor the panel to the right of the map, just under the topbar.
      // This keeps it out of the way of the bottom-left legend, the
      // bottom-center travel mode strip, and the metric strip — and it
      // tucks against the inspector when the inspector is open instead
      // of overlapping the map's main viewport.
      pop.style.top = '68px';
      pop.style.bottom = 'auto';
      pop.style.left = 'auto';
      pop.style.right = (document.body.getAttribute('data-inspector-open') === 'true')
        ? `calc(var(--inspector-w, 320px) + 16px)`
        : '16px';
      pop.setAttribute('data-open', 'true');
      renderChangesItems();
    } else {
      pop.setAttribute('data-open', 'false');
    }
    const railBtn = document.getElementById('rail-changes');
    if (railBtn) railBtn.setAttribute('data-active', isOpen ? 'false' : 'true');
  }

  function renderChangesItems() {
    const body = document.getElementById('shellChangesBody');
    const meta = document.querySelector('#shellChanges .changes-meta');
    if (!body) return;
    const log = (typeof whatIfChangeLog !== 'undefined' && Array.isArray(whatIfChangeLog)) ? whatIfChangeLog : [];
    const cityName = (typeof lastCityName === 'string' && lastCityName) ? lastCityName.toUpperCase() : '—';
    const sourceName = (typeof sourceMode === 'string' && sourceMode) ? sourceMode.toUpperCase() : 'OSM';
    if (meta) meta.textContent = `· ${cityName} · ${sourceName}`;

    if (!log.length) {
      body.innerHTML = '<div class="changes-empty">No changes recorded yet.</div>';
      return;
    }
    // Newest first.
    const items = [...log].reverse().map(rec => {
      const label = rec.description || rec.label || `Change #${rec.id}`;
      const sub = [
        rec.category ? prettyCatLabel(rec.category) : '',
        Number.isFinite(rec.beforeGini) && Number.isFinite(rec.afterGini)
          ? `Gini ${rec.beforeGini.toFixed(3)} → ${rec.afterGini.toFixed(3)}` : ''
      ].filter(Boolean).join(' · ');
      const t = rec.timestamp instanceof Date ? rec.timestamp : (rec.timestamp ? new Date(rec.timestamp) : null);
      const time = t ? t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
      const delta = Number.isFinite(rec.giniDelta) ? rec.giniDelta : null;
      const dir = delta == null ? 'zero' : delta > 0 ? 'up' : delta < 0 ? 'down' : 'zero';
      const deltaText = delta != null ? `${delta > 0 ? '+' : ''}${delta.toFixed(3)}` : '±—';
      const isPinned = (typeof pinnedChangeId !== 'undefined' && rec.id === pinnedChangeId);
      const hasFeatures = (rec.colorPairs?.length > 0) || rec.highlightCenter;
      return `
        <div class="change-row" data-id="${rec.id}" data-active="${isPinned ? 'true' : 'false'}">
          <div class="change-main">
            <div class="change-label">${escapeHTML(label)}</div>
            ${sub ? `<div class="change-sub tiny muted">${escapeHTML(sub)}</div>` : ''}
          </div>
          <div class="change-meta">
            ${hasFeatures ? `<button class="btn-toggle-vis" data-on="${isPinned ? 'true' : 'false'}" data-id="${rec.id}" type="button">
              ${isPinned ? svg('eyeOff', 11) + ' Hide' : svg('eye', 11) + ' Show'}
            </button>` : ''}
            <span class="delta-pill" data-dir="${dir}">${deltaText}</span>
            <span class="tiny muted change-time">${time}</span>
          </div>
        </div>`;
    }).join('');
    body.innerHTML = items;

    // Wire Show/Hide toggles to the existing highlightChangeOnMap function.
    body.querySelectorAll('.btn-toggle-vis').forEach(btn => {
      btn.addEventListener('click', (ev) => {
        ev.stopPropagation();
        const id = Number(btn.dataset.id);
        const rec = log.find(r => r.id === id);
        if (rec && typeof highlightChangeOnMap === 'function') {
          highlightChangeOnMap(rec);
          // Re-render so the pin state and button labels stay in sync.
          renderChangesItems();
        }
      });
    });
  }

  function prettyCatLabel(cat) {
    return (typeof prettyPOIName === 'function')
      ? prettyPOIName(cat)
      : String(cat).replace(/_/g, ' ');
  }

  function wireChangesPopover() {
    const pop = document.getElementById('shellChanges');
    if (!pop) return;
    pop.querySelector('[data-close="changes"]')?.addEventListener('click', () => {
      pop.setAttribute('data-open', 'false');
      const railBtn = document.getElementById('rail-changes');
      if (railBtn) railBtn.setAttribute('data-active', 'false');
    });
    pop.querySelector('#shellChangesClear')?.addEventListener('click', () => {
      const legacy = document.getElementById('changeLogClearBtn');
      if (legacy) legacy.click();
      else if (typeof whatIfChangeLog !== 'undefined' && Array.isArray(whatIfChangeLog)) {
        whatIfChangeLog.length = 0;
        if (typeof updateChangeLogUI === 'function') updateChangeLogUI();
      }
      // Drop pinned highlight if any.
      if (typeof pinnedChangeId !== 'undefined' && pinnedChangeId != null) {
        try { pinnedChangeId = null; if (typeof updateLayers === 'function') updateLayers(); } catch (_) {}
      }
      renderChangesItems();
    });
    pop.querySelector('#shellChangesCompare')?.addEventListener('click', () => {
      // Toggle the "compare-baseline" mode if the legacy state is exposed.
      try {
        if (typeof changeCompareBaseline !== 'undefined') {
          changeCompareBaseline = !changeCompareBaseline;
          if (typeof updateLayers === 'function') updateLayers();
        }
      } catch (_) {}
      renderChangesItems();
    });
  }

  // Update both the History and Changes popovers when the legacy log mutates.
  // updateChangeLogUI is called whenever an entry is added/removed/toggled.
  function patchChangeLogUI() {
    if (typeof updateChangeLogUI !== 'function' || updateChangeLogUI.__shellPatched) return;
    const original = updateChangeLogUI;
    const patched = function (...args) {
      const result = original.apply(this, args);
      try { renderChangesItems(); } catch (_) {}
      try { renderHistoryItems(); } catch (_) {}
      return result;
    };
    patched.__shellPatched = true;
    // Replace the global so all callers go through the patched version.
    try { window.updateChangeLogUI = patched; } catch (_) {}
    try { globalThis.updateChangeLogUI = patched; } catch (_) {}
  }

  function toggleHistoryPopover() {
    const pop = document.getElementById('shellHistory');
    if (!pop) return;
    const isOpen = pop.getAttribute('data-open') === 'true';
    closeAllPopovers();
    if (!isOpen) {
      // Anchor history popover bottom-right (matches the design's HistoryStack
      // popover, and stays out of the way of the rail/inspector).
      pop.style.left = 'auto';
      pop.style.top = 'auto';
      pop.style.right = (document.body.getAttribute('data-inspector-open') === 'true')
        ? 'calc(var(--inspector-w, 320px) + 16px)'
        : '16px';
      pop.style.bottom = '16px';
      pop.setAttribute('data-open', 'true');
      renderHistoryItems();
    }
    const railBtn = document.getElementById('rail-history');
    if (railBtn) railBtn.setAttribute('data-active', isOpen ? 'false' : 'true');
  }

  function renderHistoryItems() {
    const body = document.getElementById('shellHistoryBody');
    if (!body) return;
    // Source of truth = history.js snapshot stack (restorable states), NOT the
    // Changes log (whatIfChangeLog) — that one has its own popover.
    const stack = Array.isArray(window.historyStack) ? window.historyStack : [];
    if (!stack.length) {
      body.innerHTML = '<div class="history-empty">No states captured yet. Compute fairness, switch modes, or make a what-if change — snapshots appear here; click one to jump back.</div>';
      return;
    }
    const cursor = (typeof window.historyCursor === 'function') ? window.historyCursor() : -1;
    // Newest first; data-idx keeps the real stack index for historyRestore().
    const items = stack.map((snap, idx) => {
      const label = snap.label || `State #${idx + 1}`;
      const t = snap.timestamp ? new Date(snap.timestamp) : null;
      const time = t ? t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
      return `
        <div class="history-item" data-idx="${idx}" data-active="${idx === cursor ? 'true' : 'false'}" title="Restore this state">
          <span class="num">${idx + 1}</span>
          <span class="label">${escapeHTML(label)}</span>
          <span class="time">${time}</span>
        </div>`;
    }).reverse().join('');
    body.innerHTML = items;
  }

  function escapeHTML(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function wireHistoryPopover() {
    const pop = document.getElementById('shellHistory');
    if (!pop) return;
    pop.querySelector('[data-close="history"]')?.addEventListener('click', () => {
      pop.setAttribute('data-open', 'false');
      const railBtn = document.getElementById('rail-history');
      if (railBtn) railBtn.setAttribute('data-active', 'false');
    });
    pop.querySelector('#shellHistoryClear')?.addEventListener('click', () => {
      // Clears the snapshot stack (history.js), not the Changes log.
      if (typeof window.historyClear === 'function') window.historyClear();
      renderHistoryItems();
    });
    // Row click = restore that snapshot (delegated; rows re-render often).
    pop.querySelector('#shellHistoryBody')?.addEventListener('click', (ev) => {
      const item = ev.target.closest('.history-item[data-idx]');
      if (!item) return;
      const idx = Number(item.getAttribute('data-idx'));
      if (Number.isFinite(idx) && typeof window.historyRestore === 'function') {
        window.historyRestore(idx);
      }
    });
    // history.js pings this whenever the stack mutates (capture/restore/clear).
    window.onHistoryStackChanged = () => { try { renderHistoryItems(); } catch (_) {} };
  }

  function toggleAISidebar() {
    const a = document.getElementById('shellAI');
    if (!a) return;
    const open = a.getAttribute('data-open') === 'true';
    // AI sidebar and Inspector are mutually exclusive (both occupy the
    // right edge). Closing the inspector first prevents both panels from
    // overlapping each other.
    if (!open) {
      const insp = document.getElementById('inspector');
      if (insp && insp.getAttribute('data-open') === 'true') toggleInspector();
    }
    a.setAttribute('data-open', open ? 'false' : 'true');
    document.body.setAttribute('data-ai-open', open ? 'false' : 'true');
    const railBtn = document.getElementById('rail-ai');
    if (railBtn) railBtn.setAttribute('data-active', open ? 'false' : 'true');
    scheduleMapResize();
  }

  // ---------------- city menu wiring ----------------
  function syncCityMenu() {
    const legacyCity = document.getElementById('citySelect');
    const current = legacyCity?.value || 'vaxjo';
    document.querySelectorAll('#shellCityMenu .city-row').forEach(b => {
      b.setAttribute('data-active', b.dataset.key === current ? 'true' : 'false');
    });
    const lbl = document.getElementById('topCityLabel');
    if (lbl) {
      const c = CITIES.find(x => x.key === current);
      if (c) lbl.textContent = c.label;
    }
  }
  function pickCity(key) {
    const legacyCity = document.getElementById('citySelect');
    if (!legacyCity) return;
    legacyCity.value = key;
    legacyCity.dispatchEvent(new Event('change', { bubbles: true }));
    syncCityMenu();
    document.getElementById('shellCityMenu').setAttribute('data-open', 'false');
  }
  function syncJobStatus() {
    const legacy = document.getElementById('jobStatus');
    const el = document.getElementById('topJobStatus');
    if (legacy && el) el.textContent = legacy.textContent;
  }

  // ---------------- POI popover wiring ----------------
  function syncPOIPopoverFromLegacy() {
    POI_META.forEach(m => {
      const legacy = document.getElementById('poi_' + m.id);
      const shellChk = document.querySelector(`#shellPOIPopover .shell-poi-check[data-cat="${m.id}"]`);
      const shellRng = document.querySelector(`#shellPOIPopover .shell-poi-weight[data-cat="${m.id}"]`);
      const shellNum = document.querySelector(`#shellPOIPopover .shell-poi-numb[data-cat="${m.id}"]`);
      const shellRow = document.querySelector(`#shellPOIPopover .poi-row[data-cat="${m.id}"]`);
      if (!legacy || !shellChk) return;
      shellChk.checked = legacy.checked;
      if (shellRng) shellRng.disabled = !legacy.checked;
      if (shellRow) shellRow.setAttribute('data-checked', legacy.checked ? 'true' : 'false');
      const lwEl = document.querySelector(`.poi-weight[data-cat="${m.id}"]`);
      if (lwEl && shellRng) shellRng.value = lwEl.value;
      if (lwEl && shellNum) shellNum.textContent = lwEl.value;
    });
    const sym = document.getElementById('poiSymbolsToggle');
    const ssym = document.getElementById('shellPOISymbolsToggle');
    if (sym && ssym) ssym.checked = sym.checked;
  }
  function wirePOIPopover() {
    const pop = document.getElementById('shellPOIPopover');
    if (!pop) return;
    pop.querySelectorAll('.shell-poi-check').forEach(chk => {
      chk.addEventListener('change', () => {
        const cat = chk.dataset.cat;
        const legacy = document.getElementById('poi_' + cat);
        if (legacy) {
          legacy.checked = chk.checked;
          legacy.dispatchEvent(new Event('change', { bubbles: true }));
        }
        const shellRng = document.querySelector(`#shellPOIPopover .shell-poi-weight[data-cat="${cat}"]`);
        const shellRow = document.querySelector(`#shellPOIPopover .poi-row[data-cat="${cat}"]`);
        if (shellRng) shellRng.disabled = !chk.checked;
        if (shellRow) shellRow.setAttribute('data-checked', chk.checked ? 'true' : 'false');
      });
    });
    pop.querySelectorAll('.shell-poi-weight').forEach(rng => {
      rng.addEventListener('input', () => {
        const cat = rng.dataset.cat;
        const legacy = document.querySelector(`.poi-weight[data-cat="${cat}"]`);
        if (legacy) {
          legacy.value = rng.value;
          legacy.dispatchEvent(new Event('input', { bubbles: true }));
          legacy.dispatchEvent(new Event('change', { bubbles: true }));
        }
        const num = document.querySelector(`#shellPOIPopover .shell-poi-numb[data-cat="${cat}"]`);
        if (num) num.textContent = rng.value;
      });
    });
    pop.querySelector('#shellPOIClear')?.addEventListener('click', () => {
      const legacy = document.getElementById('poiClearBtn');
      if (legacy) legacy.click();
      setTimeout(syncPOIPopoverFromLegacy, 0);
    });
    pop.querySelector('#shellPOIAll')?.addEventListener('click', () => {
      POI_META.forEach(m => {
        const legacy = document.getElementById('poi_' + m.id);
        if (legacy && !legacy.checked) {
          legacy.checked = true;
          legacy.dispatchEvent(new Event('change', { bubbles: true }));
        }
      });
      setTimeout(syncPOIPopoverFromLegacy, 0);
    });
    pop.querySelector('#shellPOISymbolsToggle')?.addEventListener('change', e => {
      const sym = document.getElementById('poiSymbolsToggle');
      if (sym) {
        sym.checked = e.target.checked;
        sym.dispatchEvent(new Event('change', { bubbles: true }));
      }
    });
    pop.querySelector('[data-close="poi"]')?.addEventListener('click', () => pop.setAttribute('data-open', 'false'));
  }

  // ---------------- AI sidebar wiring ----------------
  function wireAISidebar() {
    const a = document.getElementById('shellAI');
    if (!a) return;
    a.querySelector('[data-close="ai"]')?.addEventListener('click', toggleAISidebar);
    const send = a.querySelector('#shellAISend');
    const input = a.querySelector('#shellAIInput');
    const body = a.querySelector('#shellAIBody');
    function appendMsg(text, who) {
      const div = document.createElement('div');
      div.className = 'ai-msg' + (who === 'user' ? ' user' : '');
      div.textContent = text;
      body.appendChild(div);
      body.scrollTop = body.scrollHeight;
    }
    function fire() {
      const txt = (input.value || '').trim();
      if (!txt) return;
      appendMsg(txt, 'user');
      input.value = '';
      // Proxy to legacy chatbot input if present.
      const legacyInput = document.querySelector('#chatbotPanel textarea, #chatbotPanel input[type="text"]');
      const legacyForm = document.querySelector('#chatbotPanel form');
      if (legacyInput) {
        legacyInput.value = txt;
        legacyInput.dispatchEvent(new Event('input', { bubbles: true }));
      }
      if (legacyForm) {
        legacyForm.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
      } else {
        // Fallback: try a send button inside chatbotPanel.
        const sendBtn = document.querySelector('#chatbotPanel button[type="submit"], #chatbotPanel .chatbot-send');
        if (sendBtn) sendBtn.click();
      }
      // Show a placeholder reply (real one comes from existing logic if wired).
      setTimeout(() => appendMsg('Working on it…', 'assistant'), 200);
    }
    send?.addEventListener('click', fire);
    input?.addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); fire(); }
    });
    // Suggestion buttons populate the textarea and submit immediately.
    a.querySelectorAll('#shellAISuggest button[data-prompt]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (!input) return;
        input.value = btn.dataset.prompt || btn.textContent.trim();
        fire();
      });
    });
  }

  function wireTopbar() {
    document.getElementById('topCityBtn')?.addEventListener('click', toggleCityMenu);
    document.querySelectorAll('#shellCityMenu .city-row').forEach(btn => {
      btn.addEventListener('click', () => pickCity(btn.dataset.key));
    });
    document.getElementById('topResetViewBtn')?.addEventListener('click', () => {
      if (typeof resetMapView === 'function') resetMapView();
      else console.warn('[FAVE] resetMapView not available');
    });
    document.getElementById('topExportBtn')?.addEventListener('click', () => {
      // Resolved at click time: assets/js/export/* loads after this module.
      if (window.FAVEExport?.run) window.FAVEExport.run();
      else console.warn('[FAVE] vector export module not loaded');
    });
    document.getElementById('topThemeBtn')?.addEventListener('click', () => {
      const cur = document.body.getAttribute('data-theme') || 'light';
      const next = cur === 'light' ? 'dark' : 'light';
      document.body.setAttribute('data-theme', next);
      // Drive the legacy basemapStyleToggle (checked = dark) so the existing
      // change handler swaps the MapLibre style. Dispatching change fires the
      // listener installed in lib/state.js → setBasemapStyle().
      const bm = document.getElementById('basemapStyleToggle');
      if (bm) {
        // Always dispatch — setBasemapStyle is idempotent and we need it to
        // fire even if the checkbox's stored state already matches the
        // desired one (which can happen because the HTML default is
        // checked but the map starts on the light style).
        bm.checked = (next === 'dark');
        bm.dispatchEvent(new Event('change', { bubbles: true }));
      }
    });
    document.getElementById('topHelpBtn')?.addEventListener('click', toggleHelpModal);
    document.querySelectorAll('.fave-topbar .segmented [data-model]').forEach(b => {
      b.addEventListener('click', () => {
        document.querySelectorAll('.fave-topbar .segmented [data-model]').forEach(x => x.setAttribute('data-active', 'false'));
        b.setAttribute('data-active', 'true');
        const mode = b.dataset.model === 'distance' ? 'default' : 'ifcity';
        if (typeof setFairnessModel === 'function') {
          // recompute:true → recomputeFairnessAfterWhatIf runs the category
          // computation (when fairActive) and always calls autoComputeOverall.
          setFairnessModel(mode, { recompute: true });
        }
      });
    });
    syncCityMenu();
    syncJobStatus();
    new MutationObserver(syncJobStatus).observe(document.getElementById('jobStatus') || document.body, { childList: true, subtree: true, characterData: true });
  }

  function wireTravelMode(strip) {
    strip.querySelectorAll('button[data-mode]').forEach(btn => {
      btn.addEventListener('click', () => {
        const mode = btn.dataset.mode;
        strip.querySelectorAll('button[data-mode]').forEach(b => b.setAttribute('data-active', b.dataset.mode === mode ? 'true' : 'false'));
        // Drive the legacy state. Default recompute=true so per-building
        // fairness re-runs (recomputeFairnessAfterWhatIf) and the map
        // recolours. Also dispatch the legacy <select> change event so any
        // listeners on #fairnessTravelMode fire.
        if (typeof setFairnessTravelMode === 'function') {
          setFairnessTravelMode(mode);
        }
        const sel = document.getElementById('fairnessTravelMode');
        if (sel) {
          sel.value = mode;
          sel.dispatchEvent(new Event('change', { bubbles: true }));
        }
        // Belt-and-braces: if fairness isn't active yet, refresh the
        // overall colouring so the map still reacts to the mode change.
        if (typeof autoComputeOverall === 'function') autoComputeOverall();
      });
    });
  }
  function wireZoom(zoom) {
    zoom.querySelector('#shellZoomIn')?.addEventListener('click', () => { if (typeof map !== 'undefined' && map?.zoomIn) map.zoomIn(); });
    zoom.querySelector('#shellZoomOut')?.addEventListener('click', () => { if (typeof map !== 'undefined' && map?.zoomOut) map.zoomOut(); });
  }

  // Floating "Clear selection" pill — visible whenever DR/PC/map have
  // an active selection. Polls for selection state since selection
  // mutations happen in many places (DR lasso, PC click, district click,
  // map lasso) and the legacy code does not emit a single event.
  function wireClearSelectionPill() {
    const pill = document.getElementById('clearSelectionPill');
    const count = document.getElementById('clearSelectionPillCount');
    if (!pill) return;
    pill.addEventListener('click', () => {
      // Mirror the existing Clear button in the DR toolbar — covers
      // persistent building selection + Parallel Coords selection + DR
      // lasso state. Then click the map-lasso Clear so the map overlay
      // also resets.
      try { if (typeof clearSelection === 'function') clearSelection(); } catch (_) {}
      try { if (typeof clearParallelCoordsSelectionFromClearAction === 'function') clearParallelCoordsSelectionFromClearAction(); } catch (_) {}
      try { if (typeof clearDRMapSelection === 'function') clearDRMapSelection(); } catch (_) {}
      try { if (typeof updateLayers === 'function') updateLayers(); } catch (_) {}
      const mapClear = document.getElementById('mapLassoClearBtn');
      if (mapClear && !mapClear.disabled) mapClear.click();
      try { window.faveInspector?.clearSelection?.(); } catch (_) {}
      pill.hidden = true;
    });
    // Cheap polling — selection-state surfaces vary across files; one
    // 400 ms tick is enough to keep the pill in sync without wiring
    // listeners into every selection mutator.
    setInterval(() => {
      let n = 0;
      try {
        if (typeof persistentBuildingSelection !== 'undefined' && persistentBuildingSelection?.size) {
          n = persistentBuildingSelection.size;
        } else if (typeof baseCityFC !== 'undefined' && Array.isArray(baseCityFC?.features)) {
          n = baseCityFC.features.reduce(
            (acc, f) => acc + (f?.properties?._drSelected ? 1 : 0), 0);
        }
        // Districts/cells also count as a selection when they're the
        // only thing flagged (e.g. macro click).
        if (n === 0 && typeof districtFC !== 'undefined') {
          const ds = (districtFC?.features || []).filter(
            f => f?.properties?._drSelected).length;
          if (ds) n = ds;
        }
        if (n === 0 && typeof mezoHexData !== 'undefined') {
          const ms = (mezoHexData || []).filter(
            c => c?._drSelected).length;
          if (ms) n = ms;
        }
      } catch (_) { n = 0; }
      pill.hidden = n === 0;
      if (count) count.textContent = n ? `· ${n.toLocaleString()}` : '';
    }, 400);
  }

  // Wire the new design's DR Explorer algorithm segmented buttons. They
  // mirror the hidden <select id="drAlgo"> so legacy drView.js (which
  // reads the select by ID) keeps working unchanged.
  function wireDRDesignToolbar() {
    const algoSelect = document.getElementById('drAlgo');
    const btns = document.querySelectorAll('.dr-algo-btn');
    if (!algoSelect || !btns.length) return;
    const setActive = (algo) => {
      btns.forEach(b => b.setAttribute('data-active', b.dataset.algo === algo ? 'true' : 'false'));
      const lbl = document.getElementById('drPlotLabel');
      if (lbl) lbl.textContent = `${algo.toUpperCase()} embedding`;
      // Pane header sub-label (e.g. "UMAP" / "PCA").
      const sub = document.getElementById('drPaneSub');
      if (sub) sub.textContent = algo.toUpperCase();
    };
    btns.forEach(btn => {
      btn.addEventListener('click', () => {
        const algo = btn.dataset.algo;
        if (!algo) return;
        algoSelect.value = algo;
        algoSelect.dispatchEvent(new Event('change', { bubbles: true }));
        setActive(algo);
      });
    });
    setActive(algoSelect.value || 'umap');
  }

  function watchInspector() {
    const insp = document.getElementById('inspector');
    if (!insp) return;
    new MutationObserver(syncInspectorBody).observe(insp, { attributes: true, attributeFilter: ['data-open'] });
    syncInspectorBody();
  }

  function syncDrawerBodyAttrs() {
    const dr = document.getElementById('drOffcanvas');
    const pc = document.getElementById('parallelCoordsPanel');
    const drOpen = !!(dr && dr.classList.contains('show'));
    const pcOpen = !!(pc && !pc.classList.contains('d-none'));
    document.body.setAttribute('data-dr-open', drOpen ? 'true' : 'false');
    document.body.setAttribute('data-pc-open', pcOpen ? 'true' : 'false');
    // When everything closes, also drop the collapsed state so reopening
    // doesn't leave the new drawer hidden behind the collapsed strip.
    if (!drOpen && !pcOpen) document.body.setAttribute('data-drawer-collapsed', 'false');
    // Mirror active state on rail buttons so the accent stripe lights up
    // for whichever views are currently visible in the drawer.
    const railDR = document.getElementById('rail-dr');
    if (railDR) railDR.setAttribute('data-active', drOpen ? 'true' : 'false');
    const railPC = document.getElementById('rail-parallel');
    if (railPC) railPC.setAttribute('data-active', pcOpen ? 'true' : 'false');
    // Layout: when DR is alone, use the design's default side-by-side grid
    // (.dr-explorer 1fr/380px). When DR + PC are both open, switch to the
    // vertical stacked layout (.dr-explorer-stacked) so the split-pane
    // narrow widths read better. Toggling the class via JS is more
    // reliable than relying on body[data-pc-open] descendant selectors,
    // which were not flipping the layout for some users.
    const drExp = document.querySelector('#drOffcanvas .dr-explorer');
    if (drExp) {
      drExp.classList.toggle('dr-explorer-stacked', pcOpen);
    }
    syncDrawerBar();
  }

  // --- Drawer resize / collapse polish ---------------------------------
  // Adds a thin draggable handle at the top edge of each bottom drawer so
  // users can resize the drawer height by dragging. Updates --drawer-h on
  // <html> so both #drOffcanvas and #parallelCoordsPanel stay in sync.
  function installDrawerHandle(drawerEl) {
    if (!drawerEl || drawerEl.querySelector('.shell-drawer-handle')) return;
    const handle = document.createElement('div');
    handle.className = 'shell-drawer-handle';
    handle.title = 'Drag to resize';
    handle.innerHTML = '<span class="grip"></span>';
    drawerEl.appendChild(handle);

    let dragging = false;
    let startY = 0;
    let startH = 0;
    const onMove = (e) => {
      if (!dragging) return;
      const dy = startY - e.clientY;
      const next = Math.max(160, Math.min(window.innerHeight - 120, startH + dy));
      document.documentElement.style.setProperty('--drawer-h', `${next}px`);
      try { if (typeof scheduleMapResize === 'function') scheduleMapResize(); } catch (_) {}
    };
    const onUp = () => {
      dragging = false;
      handle.removeAttribute('data-dragging');
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      dragging = true;
      startY = e.clientY;
      const cssVal = getComputedStyle(document.documentElement).getPropertyValue('--drawer-h').trim();
      startH = parseFloat(cssVal) || drawerEl.getBoundingClientRect().height;
      handle.setAttribute('data-dragging', 'true');
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }

  // The DR Explorer is a LEFT side-panel — it gets a right-edge WIDTH handle
  // (updates --dr-panel-w) instead of the bottom drawer's top-edge height handle.
  function installDrPanelWidthHandle(panelEl) {
    if (!panelEl || panelEl.querySelector('.dr-panel-resize')) return;
    const handle = document.createElement('div');
    handle.className = 'dr-panel-resize';
    handle.title = 'Drag to widen';
    handle.innerHTML = '<span class="grip"></span>';
    panelEl.appendChild(handle);

    let dragging = false;
    let startX = 0;
    let startW = 0;
    const railW = () => {
      const v = getComputedStyle(document.documentElement).getPropertyValue('--rail-w').trim();
      return parseFloat(v) || 56;
    };
    const onMove = (e) => {
      if (!dragging) return;
      const dx = e.clientX - startX;
      const maxW = window.innerWidth - railW() - 40;
      const next = Math.max(360, Math.min(maxW, startW + dx));
      document.documentElement.style.setProperty('--dr-panel-w', `${next}px`);
      try { if (typeof scheduleMapResize === 'function') scheduleMapResize(); } catch (_) {}
    };
    const onUp = () => {
      dragging = false;
      handle.removeAttribute('data-dragging');
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      dragging = true;
      startX = e.clientX;
      const cssVal = getComputedStyle(document.documentElement).getPropertyValue('--dr-panel-w').trim();
      startW = parseFloat(cssVal) || panelEl.getBoundingClientRect().width;
      handle.setAttribute('data-dragging', 'true');
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  }

  function ensureDrawerHandles() {
    installDrPanelWidthHandle(document.getElementById('drOffcanvas'));
    installDrawerHandle(document.getElementById('parallelCoordsPanel'));
  }

  // Floating toolbar that summarises which drawer panes are open and offers
  // a single collapse/expand control. Lives at the top of the active drawer
  // surface, above the rail/inspector cutouts.
  function ensureDrawerBar() {
    if (document.getElementById('shellDrawerBar')) return;
    const bar = document.createElement('div');
    bar.id = 'shellDrawerBar';
    bar.className = 'shell-drawer-bar';
    bar.innerHTML = `
      <button class="icon-btn" id="shellDrawerCollapse" type="button" title="Collapse / expand drawer" aria-label="Collapse drawer">
        ${svg('chev', 12)}
      </button>
      <span class="bar-label">Bottom drawer · drag top edge to resize</span>
      <span class="bar-count" id="shellDrawerCount"></span>
    `;
    document.body.appendChild(bar);
    bar.querySelector('#shellDrawerCollapse').addEventListener('click', () => {
      const collapsed = document.body.getAttribute('data-drawer-collapsed') === 'true';
      document.body.setAttribute('data-drawer-collapsed', collapsed ? 'false' : 'true');
      try { scheduleMapResize(); } catch (_) {}
    });
  }

  function syncDrawerBar() {
    const bar = document.getElementById('shellDrawerBar');
    if (!bar) return;
    // The bottom drawer bar belongs to the Parallel-Coords bottom drawer only.
    // The DR Explorer is now a left side-panel and manages its own chrome.
    const pcOpen = document.body.getAttribute('data-pc-open') === 'true';
    bar.setAttribute('data-shown', pcOpen ? 'true' : 'false');
    const lbl = bar.querySelector('#shellDrawerCount');
    if (lbl) lbl.textContent = pcOpen ? '1 pane' : '';
  }

  function watchDrawers() {
    ensureDrawerBar();
    ensureDrawerHandles();
    const dr = document.getElementById('drOffcanvas');
    if (dr) {
      // DR is a FIXED left overlay — opening/closing it never changes the map
      // container size (only the legend/PC panel shift, via CSS). So DON'T call
      // scheduleMapResize() here: a spurious map.resize()+deck.redraw made the
      // map's POI icons flicker off/on every time the panel slid in or out.
      dr.addEventListener('shown.bs.offcanvas', () => { syncDrawerBodyAttrs(); });
      dr.addEventListener('hidden.bs.offcanvas', () => { syncDrawerBodyAttrs(); });
    }
    const pc = document.getElementById('parallelCoordsPanel');
    if (pc) {
      new MutationObserver(() => { syncDrawerBodyAttrs(); scheduleMapResize(); })
        .observe(pc, { attributes: true, attributeFilter: ['class'] });
    }
    syncDrawerBodyAttrs();
  }

  document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('faveTopbar')) return;

    document.body.appendChild(buildTopbar());
    document.body.appendChild(buildRail());
    document.body.appendChild(buildLegend());
    document.body.appendChild(buildZoom());
    const travel = buildTravel();
    document.body.appendChild(travel);
    document.body.appendChild(buildCityMenu());
    document.body.appendChild(buildPOIPopover());
    document.body.appendChild(buildHistoryPopover());
    document.body.appendChild(buildChangesPopover());
    document.body.appendChild(buildAISidebar());

    wireTopbar();
    wireTravelMode(travel);
    wireZoom(document.getElementById('shellZoom'));
    wirePOIPopover();
    wireHistoryPopover();
    wireChangesPopover();
    wireAISidebar();
    wireDRDesignToolbar();
    wireClearSelectionPill();
    wireTooltipEvents();
    watchInspector();
    watchDrawers();
    syncScaleActiveStates();
    syncPOIRailActive();
    // Mirror POI checkbox state onto rail-poi instantly. Both the legacy
    // navbar checkboxes and the shell popover's mirror checkboxes feed
    // into the same selection (the shell ones dispatch 'change' on the
    // legacy ones), so listening on .poi-check covers both UIs.
    document.querySelectorAll('.poi-check').forEach(el => {
      el.addEventListener('change', syncPOIRailActive);
    });
    // Patch updateChangeLogUI so any change-log mutation refreshes the
    // shell popovers in lockstep with the legacy dropdown.
    patchChangeLogUI();
    // Initial resize once map exists (covers the page-load case where
    // #mapContainer's bbox changes after the deck.gl canvas mounts).
    setTimeout(scheduleMapResize, 200);
    setTimeout(scheduleMapResize, 800);

    // Click-outside closes popovers.
    document.addEventListener('mousedown', e => {
      const target = e.target;
      if (target.closest('.shell-popover')) return;
      // ignore clicks on triggers themselves (they'll toggle via their own handler)
      if (target.closest('#topCityBtn') || target.closest('#rail-poi') || target.closest('#rail-history') || target.closest('#rail-wif') || target.closest('#rail-changes')) return;
      closeAllPopovers();
    });

    // Periodically resync SCALE active states + city/job status.
    let lastDistrict = null, lastMezo = null;
    setInterval(() => {
      const dV = (typeof districtView !== 'undefined' ? districtView : false);
      const mV = (typeof mezoView !== 'undefined' ? mezoView : false);
      if (dV !== lastDistrict || mV !== lastMezo) {
        lastDistrict = dV; lastMezo = mV;
        syncScaleActiveStates();
      }
      syncJobStatus();
      syncCityMenu();
    }, 600);
  });
})();
